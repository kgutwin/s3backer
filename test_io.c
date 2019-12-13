
/*
 * s3backer - FUSE-based single file backing store via Amazon S3
 * 
 * Copyright 2008-2011 Archie L. Cobbs <archie@dellroad.org>
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 *
 * In addition, as a special exception, the copyright holders give
 * permission to link the code of portions of this program with the
 * OpenSSL library under certain conditions as described in each
 * individual source file, and distribute linked combinations including
 * the two.
 *
 * You must obey the GNU General Public License in all respects for all
 * of the code used other than OpenSSL. If you modify file(s) with this
 * exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do
 * so, delete this exception statement from your version. If you delete
 * this exception statement from all source files in the program, then
 * also delete it here.
 */

#include "s3backer.h"
#include "http_io.h"
#include "block_part.h"
#include "test_io.h"
#include "snapshots.h"

/* Do we want random errors? */
#define RANDOM_ERROR_PERCENT    0

/* Internal state */
struct test_io_private {
    struct http_io_conf         *config;
    u_char                      zero_block[0];
    struct snapshot_data        *snapshot;
};

/* s3backer_store functions */
static int test_io_create_threads(struct s3backer_store *s3b);
static int test_io_meta_data(struct s3backer_store *s3b, off_t *file_sizep, u_int *block_sizep);
static int test_io_set_mount_token(struct s3backer_store *s3b, int32_t *old_valuep, int32_t new_value);
static int test_io_read_block(struct s3backer_store *s3b, s3b_block_t block_num, void *dest,
  u_char *actual_md5, const u_char *expect_md5, int strict);
static int test_io_write_block(struct s3backer_store *s3b, s3b_block_t block_num, const void *src, u_char *md5,
  check_cancel_t *check_cancel, void *check_cancel_arg);
static int test_io_read_block_part(struct s3backer_store *s3b, s3b_block_t block_num, u_int off, u_int len, void *dest);
static int test_io_write_block_part(struct s3backer_store *s3b, s3b_block_t block_num, u_int off, u_int len, const void *src);
static int test_io_list_blocks(struct s3backer_store *s3b, block_list_func_t *callback, void *arg);
static int test_io_list_block_versions(struct s3backer_store *s3b, block_list_ver_func_t *callback, void *arg);
static int test_io_print_snapshots(struct s3backer_store *s3b, void *prarg, printer_t *printer);
static int test_io_write_snapshot(struct s3backer_store *s3b, time_t timestamp, char *buf, size_t bufsiz);
static int test_io_flush(struct s3backer_store *s3b);
static void test_io_destroy(struct s3backer_store *s3b);

/*
 * Constructor
 *
 * On error, returns NULL and sets `errno'.
 */
struct s3backer_store *
test_io_create(struct http_io_conf *config)
{
    struct s3backer_store *s3b;
    struct test_io_private *priv;
    int r;

    /* Initialize structures */
    if ((s3b = calloc(1, sizeof(*s3b))) == NULL) {
        r = errno;
        goto fail0;
    }

    s3b->create_threads = test_io_create_threads;
    s3b->meta_data = test_io_meta_data;
    s3b->set_mount_token = test_io_set_mount_token;
    s3b->read_block = test_io_read_block;
    s3b->write_block = test_io_write_block;
    s3b->read_block_part = test_io_read_block_part;
    s3b->write_block_part = test_io_write_block_part;
    s3b->list_blocks = test_io_list_blocks;
    s3b->list_block_versions = test_io_list_block_versions;
    s3b->print_snapshots = test_io_print_snapshots;
    s3b->write_snapshot = test_io_write_snapshot;
    s3b->flush = test_io_flush;
    s3b->destroy = test_io_destroy;
    if ((priv = calloc(1, sizeof(*priv) + config->block_size)) == NULL) {
        r = errno;
        goto fail1;
    }
    priv->config = config;
    s3b->data = priv;

    /* load snapshot if specified */
    if (config->snapshot_mount_name != NULL) {
        char path[PATH_MAX];
        int fd;

        snprintf(path, sizeof(path), "%s/.snapshots/%s", config->bucket, config->snapshot_mount_name);
        if ((fd = open(path, O_RDONLY)) == -1) {
            r = errno;
            (*config->log)(LOG_ERR, "open snapshot %s: %s", path, strerror(r));
            goto fail2;
        }
        // determine size of snapshot file, read entire contents and pass on
        priv->snapshot = snapshots_read(buf, size, timestamp);
    }
    
    /* Random initialization */
    srandom((u_int)time(NULL));

    /* Done */
    return s3b;

 fail2:
    free(priv);
 fail1:
    free(s3b);
 fail0:
    errno = r;
    return NULL;
}

static int
test_io_create_threads(struct s3backer_store *s3b)
{
    return 0;
}

static int
test_io_meta_data(struct s3backer_store *s3b, off_t *file_sizep, u_int *block_sizep)
{
    return 0;
}

static int
test_io_set_mount_token(struct s3backer_store *s3b, int32_t *old_valuep, int32_t new_value)
{
    if (old_valuep != NULL)
        *old_valuep = 0;
    return 0;
}

static int
test_io_flush(struct s3backer_store *const s3b)
{
    return 0;
}

static void
test_io_destroy(struct s3backer_store *const s3b)
{
    struct test_io_private *const priv = s3b->data;

    free(priv);
    free(s3b);
}

static int
test_io_read_block(struct s3backer_store *const s3b, s3b_block_t block_num, void *dest,
  u_char *actual_md5, const u_char *expect_md5, int strict)
{
    struct test_io_private *const priv = s3b->data;
    struct http_io_conf *const config = priv->config;
    u_char md5[MD5_DIGEST_LENGTH];
    char path[PATH_MAX];
    int zero_block;
    MD5_CTX ctx;
    int fd;
    int r;

    /* Logging */
    if (config->debug)
        (*config->log)(LOG_DEBUG, "test_io: read %0*jx started", S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num);

    /* Random delay */
    usleep((random() % 200) * 1000);

    /* Random error */
    if ((random() % 100) < RANDOM_ERROR_PERCENT) {
        (*config->log)(LOG_ERR, "test_io: random failure reading %0*jx", S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num);
        return EAGAIN;
    }

    /* Generate path */
    snprintf(path, sizeof(path), "%s/%s%0*jx", config->bucket, config->prefix, S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num);

    /* Read block */
    if ((fd = open(path, O_RDONLY)) != -1) {
        int total;

        /* Read file */
        for (total = 0; total < config->block_size; total += r) {
            if ((r = read(fd, (char *)dest + total, config->block_size - total)) == -1) {
                r = errno;
                (*config->log)(LOG_ERR, "can't read %s: %s", path, strerror(r));
                close(fd);
                return r;
            }
            if (r == 0)
                break;
        }
        close(fd);

        /* Check for short read */
        if (total != config->block_size) {
            (*config->log)(LOG_ERR, "%s: file is truncated (only read %d out of %u bytes)", path, total, config->block_size);
            return EIO;
        }

        /* Done */
        r = 0;
    } else
        r = errno;

    /* Convert ENOENT into a read of all zeroes */
    if ((zero_block = (r == ENOENT))) {
        memset(dest, 0, config->block_size);
        r = 0;
    }

    /* Check for other error */
    if (r != 0) {
        (*config->log)(LOG_ERR, "can't open %s: %s", path, strerror(r));
        return r;
    }

    /* Compute MD5 */
    if (zero_block)
        memset(md5, 0, MD5_DIGEST_LENGTH);
    else {
        MD5_Init(&ctx);
        MD5_Update(&ctx, dest, config->block_size);
        MD5_Final(md5, &ctx);
    }
    if (actual_md5 != NULL)
        memcpy(actual_md5, md5, MD5_DIGEST_LENGTH);

    /* Check expected MD5 */
    if (expect_md5 != NULL) {
        const int match = memcmp(md5, expect_md5, MD5_DIGEST_LENGTH) == 0;

        if (strict) {
            if (!match) {
                (*config->log)(LOG_ERR,
                   "%s: wrong MD5 checksum?! %02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
                   " != %02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x", path,
                  (u_int)md5[0], (u_int)md5[1], (u_int)md5[2], (u_int)md5[3],
                  (u_int)md5[4], (u_int)md5[5], (u_int)md5[6], (u_int)md5[7],
                  (u_int)md5[8], (u_int)md5[9], (u_int)md5[10], (u_int)md5[11],
                  (u_int)md5[12], (u_int)md5[13], (u_int)md5[14], (u_int)md5[15],
                  (u_int)expect_md5[0], (u_int)expect_md5[1], (u_int)expect_md5[2], (u_int)expect_md5[3],
                  (u_int)expect_md5[4], (u_int)expect_md5[5], (u_int)expect_md5[6], (u_int)expect_md5[7],
                  (u_int)expect_md5[8], (u_int)expect_md5[9], (u_int)expect_md5[10], (u_int)expect_md5[11],
                  (u_int)expect_md5[12], (u_int)expect_md5[13], (u_int)expect_md5[14], (u_int)expect_md5[15]);
                return EINVAL;
            }
        } else if (match)
            r = EEXIST;
    }

    /* Logging */
    if (config->debug) {
        (*config->log)(LOG_DEBUG,
          "test_io: read %0*jx complete, MD5 %02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%s%s",
          S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num,
          (u_int)md5[0], (u_int)md5[1], (u_int)md5[2], (u_int)md5[3],
          (u_int)md5[4], (u_int)md5[5], (u_int)md5[6], (u_int)md5[7],
          (u_int)md5[8], (u_int)md5[9], (u_int)md5[10], (u_int)md5[11],
          (u_int)md5[12], (u_int)md5[13], (u_int)md5[14], (u_int)md5[15],
          zero_block ? " (zero)" : "", r == EEXIST ? " (expected md5 match)" : "");
    }

    /* Done */
    return r;
}

static int
test_io_write_block(struct s3backer_store *const s3b, s3b_block_t block_num, const void *src, u_char *caller_md5,
  check_cancel_t *check_cancel, void *check_cancel_arg)
{
    struct test_io_private *const priv = s3b->data;
    struct http_io_conf *const config = priv->config;
    char block_hash_buf[S3B_BLOCK_NUM_DIGITS + 2];
    u_char md5[MD5_DIGEST_LENGTH];
    char temp[PATH_MAX];
    char path[PATH_MAX];
    char vpath[PATH_MAX];
    char version_id[TESTIO_VERID_LEN + 1];
    MD5_CTX ctx;
    int total;
    int fd;
    int r;

    /* Check for zero block */
    if (src != NULL && memcmp(src, priv->zero_block, config->block_size) == 0)
        src = NULL;

    /* Compute MD5 */
    if (src != NULL) {
        MD5_Init(&ctx);
        MD5_Update(&ctx, src, config->block_size);
        MD5_Final(md5, &ctx);
    } else
        memset(md5, 0, MD5_DIGEST_LENGTH);

    /* Return MD5 to caller */
    if (caller_md5 != NULL)
        memcpy(caller_md5, md5, MD5_DIGEST_LENGTH);

    /* Logging */
    if (config->debug) {
        (*config->log)(LOG_DEBUG,
          "test_io: write %0*jx started, MD5 %02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%s",
          S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num,
          (u_int)md5[0], (u_int)md5[1], (u_int)md5[2], (u_int)md5[3],
          (u_int)md5[4], (u_int)md5[5], (u_int)md5[6], (u_int)md5[7],
          (u_int)md5[8], (u_int)md5[9], (u_int)md5[10], (u_int)md5[11],
          (u_int)md5[12], (u_int)md5[13], (u_int)md5[14], (u_int)md5[15],
          src == NULL ? " (zero block)" : "");
    }

    /* Random delay */
    usleep((random() % 200) * 1000);

    /* Random error */
    if ((random() % 100) < RANDOM_ERROR_PERCENT) {
        (*config->log)(LOG_ERR, "test_io: random failure writing %0*jx", S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num);
        return EAGAIN;
    }

    /* Generate paths */
    snprintf(version_id, sizeof(version_id), "%08jx%08jx",
             (uintmax_t)http_io_block_hash_prefix((s3b_block_t)time(NULL)),
             (uintmax_t)http_io_block_hash_prefix(block_num + 1));
    http_io_format_block_hash(config, block_hash_buf, sizeof(block_hash_buf), block_num);
    snprintf(path, sizeof(path), "%s/%s%s%0*jx",
      config->bucket, config->prefix, block_hash_buf, S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num);
    snprintf(vpath, sizeof(vpath), "%s/.%s.%s%s%0*jx",
             config->bucket, version_id, config->prefix, block_hash_buf, S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num);

    /* Delete zero blocks */
    if (src == NULL) {
        /* Create empty file as a 'delete marker' */
        if (config->snapshots) {
            if (open(vpath, O_CREAT|O_WRONLY, 0600) == -1) {
                r = errno;
                (*config->log)(LOG_ERR, "can't create delete marker %s: %s", vpath, strerror(r));
                return r;
            }
        }
        if (unlink(path) == -1 && errno != ENOENT) {
            r = errno;
            (*config->log)(LOG_ERR, "can't unlink %s: %s", path, strerror(r));
            return r;
        }
        return 0;
    }

    /* Write into temporary file */
    snprintf(temp, sizeof(temp), "%s.XXXXXX", path);
    if ((fd = mkstemp(temp)) == -1) {
        r = errno;
        (*config->log)(LOG_ERR, "%s: %s", temp, strerror(r));
        return r;
    }
    for (total = 0; total < config->block_size; total += r) {
        if ((r = write(fd, (const char *)src + total, config->block_size - total)) == -1) {
            r = errno;
            (*config->log)(LOG_ERR, "can't write %s: %s", temp, strerror(r));
            close(fd);
            (void)unlink(temp);
            return r;
        }
    }
    close(fd);

    /* Hard link temporary file to version */
    if (config->snapshots) {
        if (link(temp, vpath) == -1) {
            r = errno;
            (*config->log)(LOG_ERR, "can't hard link %s to %s: %s", temp, vpath, strerror(r));
            (void)unlink(temp);
            return r;
        }
    }
    
    /* Rename file */
    if (rename(temp, path) == -1) {
        r = errno;
        (*config->log)(LOG_ERR, "can't rename %s: %s", temp, strerror(r));
        (void)unlink(temp);
        return r;
    }

    /* Logging */
    if (config->debug)
        (*config->log)(LOG_DEBUG, "test_io: write %0*jx complete", S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num);

    /* Done */
    return 0;
}

static int
test_io_read_block_part(struct s3backer_store *s3b, s3b_block_t block_num, u_int off, u_int len, void *dest)
{
    struct test_io_private *const priv = s3b->data;
    struct http_io_conf *const config = priv->config;

    return block_part_read_block_part(s3b, block_num, config->block_size, off, len, dest);
}

static int
test_io_write_block_part(struct s3backer_store *s3b, s3b_block_t block_num, u_int off, u_int len, const void *src)
{
    struct test_io_private *const priv = s3b->data;
    struct http_io_conf *const config = priv->config;

    return block_part_write_block_part(s3b, block_num, config->block_size, off, len, src);
}

static int
test_io_list_blocks(struct s3backer_store *s3b, block_list_func_t *callback, void *arg)
{
    struct test_io_private *const priv = s3b->data;
    struct http_io_conf *const config = priv->config;
    s3b_block_t block_num;
    struct dirent *dent;
    DIR *dir;
    int i;

    /* Open directory */
    if ((dir = opendir(config->bucket)) == NULL)
        return errno;

    /* Scan directory */
    for (i = 0; (dent = readdir(dir)) != NULL; i++) {
        if (http_io_parse_block(config, dent->d_name, &block_num) == 0)
            (*callback)(arg, block_num);
    }

    /* Close directory */
    closedir(dir);

    /* Done */
    return 0;
}

static int
test_io_list_block_versions(struct s3backer_store *s3b, block_list_ver_func_t *callback, void *arg)
{
    struct test_io_private *const priv = s3b->data;
    struct http_io_conf *const config = priv->config;
    struct s3_block_ver block_version;
    struct dirent *dent;
    struct stat sb;
    DIR *dir;
    char path[PATH_MAX];
    char version_id[TESTIO_VERID_LEN + 1];
    int r;

    /* Open directory */
    if ((dir = opendir(config->bucket)) == NULL)
        return errno;

    /* Scan directory */
    while ((dent = readdir(dir)) != NULL) {
        if (dent->d_name[0] != '.') continue;
        if (strlen(dent->d_name) < (TESTIO_VERID_LEN + 3) || dent->d_name[TESTIO_VERID_LEN + 1] != '.') continue;
        if (http_io_parse_block(config, dent->d_name + TESTIO_VERID_LEN + 2, &block_version.block_num) != 0) continue;

        block_version.object_key = dent->d_name;
        strncpy(version_id, dent->d_name + 1, TESTIO_VERID_LEN);
        version_id[TESTIO_VERID_LEN] = '\0';
        block_version.version_id = version_id;

        snprintf(path, sizeof(path), "%s/%s", config->bucket, dent->d_name);
        if (stat(path, &sb) == -1) return errno;
        
        block_version.last_modified = sb.st_mtimespec.tv_sec;
        block_version.is_latest = (sb.st_nlink == 2);

        if ((r = (*callback)(arg, block_version)) != 0) {
            (*config->log)(LOG_ERR, "block version callback failed: %s", strerror(r));
            return r;
        }
    }

    /* Close directory */
    closedir(dir);
    
    return 0;
}

static int
test_io_print_snapshots(struct s3backer_store *s3b, void *prarg, printer_t *printer)
{
    struct test_io_private *const priv = s3b->data;
    struct http_io_conf *const config = priv->config;
    struct dirent *dent;
    DIR *dir;
    char path[PATH_MAX];
    
    (*printer)(prarg, "hello snapshots world\n");
    
    /* Open directory */
    snprintf(path, sizeof(path), "%s/.snapshots", config->bucket); 
    if ((dir = opendir(path)) == NULL) {
        (*config->log)(LOG_WARNING, "unable to open snapshot directory");
        return 1;
    }

    /* Scan directory */
    while ((dent = readdir(dir)) != NULL) {
        if (dent->d_name[0] == '.') continue;
        (*printer)(prarg, dent->d_name);
        (*printer)(prarg, "\n");
    }

    /* Close directory */
    closedir(dir);

    return 0;
}

static int
test_io_write_snapshot(struct s3backer_store *s3b, time_t timestamp, char *buf, size_t bufsiz)
{
    struct test_io_private *const priv = s3b->data;
    struct http_io_conf *const config = priv->config;
    char path[PATH_MAX];
    char filename[PATH_MAX];
    int fd;
    int r;

    /* Try to open the snapshots directory; if not, try to create it */
    snprintf(path, sizeof(path), "%s/.snapshots", config->bucket); 
    if (opendir(path) == NULL) {
        if (mkdir(path, 0700) != 0) {
            r = errno;
            (*config->log)(LOG_ERR, "unable to create snapshot directory: %s", strerror(r));
            return r;
        }
    }

    /* open snapshot file */
    strftime(filename, sizeof(filename), "%Y-%m-%dT%H:%M:%S", gmtime(&timestamp));
    snprintf(path, sizeof(path), "%s/.snapshots/%s", config->bucket, filename);
    if ((fd = open(path, O_CREAT | O_WRONLY, 0600)) == -1) {
        r = errno;
        (*config->log)(LOG_ERR, "open %s: %s", path, strerror(r));
        return r;
    }

    /* write snapshot file */
    if (write(fd, buf, bufsiz) == -1) {
        r = errno;
        (*config->log)(LOG_ERR, "cannot write %s: %s", path, strerror(r));
        close(fd);
        return r;
    }

    /* close snapshot file */
    close(fd);

    return 0;
}
