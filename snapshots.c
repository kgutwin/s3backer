
#include "s3backer.h"
#include "snapshots.h"
#include "hash.h"


/*
 * next, what's needed to mount a snapshot:
 * - config needs to have the snapshot file reference
 * - backer needs to parse that file and store the version IDs for every block
 *   - recommend an array of char* where each index is the block ID.
 *   - a NULL pointer means an empty/zero block
 * - backer needs to be able to request the appropriate version ID for each block
 * - backer should disable writes when snapshot is mounted
 */

/*
 * - add commandline / config option
 * - flush cache before taking snapshot
 * - parse snapshot timestamp
 */


static int snapshots_create_block_ver(void *arg, s3b_block_ver_t block_ver);
static void snapshots_release_block_ver(void *arg, void *entry);


/*
#define SNAPSHOTS_MAX 128


struct snapshots_private {
    struct snapshot_data *known_snapshots[SNAPSHOTS_MAX];
};


static struct snapshots_private *snap_priv;


static int
snapshots_init(void)
{
    if ((snap_priv = calloc(1, sizeof(*snap_priv))) == NULL) {
        (*config0->log)(LOG_ERR, "snapshots_init(): %s", strerror(errno));
        return errno;
    }
    
    return 0;
}
*/

/*
 * Allocate a new snapshot structure of the given block size and timestamp.
 */
static struct snapshot_data *
snapshots_allocate_data(s3b_block_t num_blocks, time_t timestamp)
{
    struct snapshot_data *sd;
    int r;
    
    /* Initialize snapshot data structure */
    if ((sd = calloc(1, sizeof(*sd))) == NULL) {
        r = errno;
        goto fail0;
    }
    sd->timestamp = timestamp;
    sd->num_blocks = num_blocks;

    if ((r = s3b_hash_create(&sd->hashtable, num_blocks)) != 0)
        goto fail1;

    /* Done */
    return sd;

 fail1:
    free(sd);
 fail0:
    //(*config->log)(LOG_ERR, "snapshot allocation failed: %s", strerror(errno));
    errno = r;
    return NULL;
}

/*
 * Free all memory associated with the given snapshot_data structure.
 */
static void
snapshots_release_data(struct snapshot_data *sd)
{
    s3b_hash_foreach(sd->hashtable, snapshots_release_block_ver, NULL);
    free(sd);
}

/*
 * Create a new snapshot at the provided timestamp from the backing store.
 */
int
snapshots_create(struct s3backer_store *const s3b, s3b_block_t num_blocks, time_t timestamp)
{
    struct snapshot_data *sd;
    char *buf = NULL;
    size_t bufend = 0;
    size_t bufsiz = 0;

    if (timestamp == 0) {
        /* the timestamp is now */
        timestamp = time(NULL);
    }
    
    if ((sd = snapshots_allocate_data(num_blocks, timestamp)) == NULL) {
        return errno;
    }
    
    if ((errno = (*s3b->list_block_versions)(s3b, snapshots_create_block_ver, sd)) != 0) {
        goto fail0;
    }

    /* Turn the snapshot_data into a string buffer */
    for (s3b_block_t i=0; i<num_blocks; i++) {
        s3b_block_ver_t *bv = s3b_hash_get(sd->hashtable, i);
        size_t delta = 1;
        
        if (bv != NULL) {
            delta = strlen(bv->version_id) + 1;
        }

        if (bufend + delta > bufsiz) {
            char *newbuf;
            if ((newbuf = realloc(buf, bufsiz + 1024)) == NULL) {
                //(*config->log)(LOG_ERR, "unable to build snapshot listing: %s", strerror(errno));
                free(buf);
                goto fail0;
            }
            buf = newbuf;
        }

        if (bv != NULL) {
            strcpy(buf + bufend, bv->version_id);
            bufend += strlen(bv->version_id);
        }
        strcpy(buf + bufend, "\n");
        bufend ++;
    }
    
    /* now save the completed snapshot back to the backing store */
    (*s3b->write_snapshot)(s3b, sd->timestamp, buf, bufend);

    free(buf);

 fail0:
    /* Deallocate the snapshot_data structure */
    snapshots_release_data(sd);

    return errno;
}

/*
 * Allocate a new block version, populated from the provided temporary block version.
 */
static int
snapshots_create_block_ver(void *arg, s3b_block_ver_t block_ver)
{
    struct snapshot_data *sd = (struct snapshot_data*)arg;
    s3b_block_ver_t *entry;
    char *buf;
    size_t bufsiz = 0;
    int r;

    /* skip this block version if the version is newer than the snapshot time */
    if (sd->timestamp > 0 && block_ver.last_modified > sd->timestamp)
        return 0;
    
    if ((entry = s3b_hash_get(sd->hashtable, block_ver.block_num)) != NULL) {
        /* Release the existing block version if it's older than the one we've been given */
        if (entry->last_modified < block_ver.last_modified) {
            s3b_hash_remove(sd->hashtable, entry->block_num);
            snapshots_release_block_ver(NULL, entry);
        } else {
            return 0;
        }
    }
    
    /* allocate and fill the new structure */
    if ((entry = calloc(1, sizeof(*entry))) == NULL) {
        r = errno;
        goto fail0;
    }
    
    entry->block_num = block_ver.block_num;
    entry->last_modified = block_ver.last_modified;
    entry->is_latest = block_ver.is_latest;

    /* copy over strings */
    if (block_ver.object_key != NULL) {
        bufsiz += strlen(block_ver.object_key) + 1;
    }
    if (block_ver.version_id != NULL) {
        bufsiz += strlen(block_ver.version_id) + 1;
    }
    if ((buf = calloc(bufsiz, sizeof(*buf))) == NULL) {
        r = errno;
        goto fail1;
    }
    if (block_ver.object_key != NULL) {
        entry->object_key = buf;
        buf = stpcpy(buf, block_ver.object_key) + 1;
    }
    if (block_ver.version_id != NULL) {
        entry->version_id = buf;
        buf = stpcpy(buf, block_ver.version_id) + 1;
    }

    s3b_hash_put(sd->hashtable, entry);
    return 0;

 fail1:
    free(entry);
 fail0:
    return r;
}

/*
 * Free memory associated with a block version.
 */
static void
snapshots_release_block_ver(void *arg, void *entry)
{
    s3b_block_ver_t *block_ver = entry;
    
    if (block_ver->object_key != NULL) {
        free(block_ver->object_key);
    } else if (block_ver->version_id != NULL) {
        free(block_ver->version_id);
    }
    free(block_ver);
}

/*
 * Read a snapshot definition file containing newline-separated version IDs, one for each block in sequence.
 */
struct snapshot_data *
snapshots_read(char *buf, size_t size, time_t timestamp)
{
    struct snapshot_data *sd;
    s3b_block_t num_blocks = 0;

    /* Count the number of newlines in buf to determine the number of blocks */
    for (int i = 0; i < size; i++) {
        if (buf[i] == '\n' || buf[i] == '\0') {
            num_blocks ++;
            buf[i] = '\0';
        }
    }

    /* Allocate a new snapshot_data structure */
    if ((sd = snapshots_allocate_data(num_blocks, timestamp)) == NULL) {
        return NULL;
    }

    /* Set up version ID pointers */
    for (int i=0, blk=0, pblk=0; i < size; i++) {
        if (buf[i] == '\0') {
            blk ++;
        } else if (pblk == blk) {
            s3b_block_ver_t block_ver;
            block_ver.block_num = blk;
            block_ver.is_latest = 1;
            block_ver.version_id = buf + i;
            snapshots_create_block_ver(sd, block_ver);
            pblk ++;
        }
    }
    
    return sd;
}
