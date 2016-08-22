USE ${keyspace};

CREATE TABLE ${table} (
    blobid blob,
    group ascii,
    chunkid int,
    data blob,
    PRIMARY KEY (blobid, group, chunkid)
) WITH COMPACT STORAGE
  AND compression = {'sstable_compression' : 'LZ4Compressor'};