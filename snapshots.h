
struct snapshot_data {
    struct s3b_hash *hashtable;
    s3b_block_t     num_blocks;    // number of blocks parsed
    time_t          timestamp;
};

extern int snapshots_create(struct s3backer_store *s3b, s3b_block_t num_blocks, time_t timestamp);
extern struct snapshot_data *snapshots_read(char *buf, size_t size, time_t timestamp);

