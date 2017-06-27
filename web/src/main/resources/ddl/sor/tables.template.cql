USE ${keyspace};

-- Use DeflateCompressor to favor smaller size over read performance and
-- disable bloom filter completely to reduced memory requirements.
CREATE TABLE IF NOT EXISTS ${table.audit} (
    rowkey blob,
    changeid timeuuid,
    audit text,
    PRIMARY KEY (rowkey, changeid)
) WITH COMPACT STORAGE
  AND bloom_filter_fp_chance = 1.0
  AND compression = {'sstable_compression' : 'DeflateCompressor'};

-- Use LZ4Compressor for maximum read performance.
-- Wish we could lower gc_grace_seconds to 5 minutes to reduce tombstones to improve read performance.
-- See proof at com.bazaarvoice.emodb.sor.core.CompactorTest.testTombstonesDoNotMatter for more details.
-- For now, we will keep the default gc_grace of 2 days.
CREATE TABLE IF NOT EXISTS ${table.delta} (
    rowkey blob,
    changeid timeuuid,
    content text,
    PRIMARY KEY (rowkey, changeid)
) WITH COMPACT STORAGE
  AND gc_grace_seconds = 172800
  AND compaction={'sstable_size_in_mb': '160', 'class': 'LeveledCompactionStrategy'}
  AND compression = {'sstable_compression' : 'LZ4Compressor'};

CREATE TABLE IF NOT EXISTS ${table.delta}_blocked (
    rowkey blob,
    changeid timeuuid,
    block int,
    content text,
    PRIMARY KEY (rowkey, changeid, block)
) WITH COMPACT STORAGE
  AND gc_grace_seconds = 172800
  AND compaction={'sstable_size_in_mb': '160', 'class': 'LeveledCompactionStrategy'}
  AND compression = {'sstable_compression' : 'LZ4Compressor'};

-- This is to save all deltas that get compacted away
CREATE TABLE IF NOT EXISTS ${table.history} (
    rowkey blob,
    changeid timeuuid,
    audit text,
    PRIMARY KEY (rowkey, changeid)
) WITH COMPACT STORAGE
  AND bloom_filter_fp_chance = 1.0
  AND compaction={'sstable_size_in_mb': '160', 'class': 'LeveledCompactionStrategy'}
  AND compression = {'sstable_compression' : 'DeflateCompressor'};
