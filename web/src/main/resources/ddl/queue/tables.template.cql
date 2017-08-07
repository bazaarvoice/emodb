USE ${keyspace};

-- One row per queue, each row contains a list of slabIds that reference rows in the 'slab' CF.
-- Slabs with "open=true" may still be in the process of being written to.  Closed slabs will never grow, just shrink
-- as messages get processed and deleted from the slabs.
-- Zero gc_grace_seconds to disable tombstones to improve read performance.  If a delete gets lost because of the lack
-- of tombstones, the application will notice and simply delete the row again.
CREATE TABLE manifest (
  queue text,
  slabid timeuuid,
  open boolean,
  PRIMARY KEY (queue, slabid)
) WITH COMPACT STORAGE
  AND gc_grace_seconds = 0
  AND compaction={'sstable_size_in_mb': '160', 'class': 'LeveledCompactionStrategy'}
  AND compression = {'sstable_compression': ''};

-- One row per slabId.  Each "slab" contains a list of messages, one message per column.  The application imposes a
-- maximum number of messages per slab, typically 1000, to avoid getting wide rows with lots of tombstones that hurt
-- read performance.  Low gc_grace_seconds to improve read performance, but not as low as with the 'manifest' CF.  If
-- a delete gets lost, the QueueService will issue a duplicate message which hurts the performance of queue consumers.
CREATE TABLE slab (
  slabid timeuuid,
  messageidx int,
  message text,
  PRIMARY KEY (slabid, messageidx)
) WITH COMPACT STORAGE
  AND compaction={'sstable_size_in_mb': '160', 'class': 'LeveledCompactionStrategy'}
  AND gc_grace_seconds = 1200
  AND compression = {'sstable_compression' : 'LZ4Compressor'};


-- Dedup Event Store's PersistentSortedQueue

-- One row per queue.  One column for each segment containing actual queue data.  The Java code recycles column uuids
-- as segments are added and deleted so, in general, tombstones shouldn't accumulate and aren't a major concern.
CREATE TABLE dedup_md (
  subscription text,
  segmentid timeuuid,
  state text,
  PRIMARY KEY (subscription, segmentid)
) WITH COMPACT STORAGE
  AND compaction={'sstable_size_in_mb': '160', 'class': 'LeveledCompactionStrategy'}
  AND compression = {'sstable_compression' : 'LZ4Compressor'};

-- One row per queue segment.  One column for each sorted & dedup'd event.  The row layout is always "[deleted columns,
-- live columns]" where the segment metadata maintains the minimum live column for each row, and all read queries start
-- from that minimum, so even if deleted columns get resurrected they won't be looked at.  That means we can safely
-- disable tombstones and let Cassandra aggressively reclaim disk space.
CREATE TABLE dedup_data (
  dataid timeuuid,
  event blob,
  unused blob,
  PRIMARY KEY (dataid, event)
) WITH COMPACT STORAGE
  AND compaction={'sstable_size_in_mb': '160', 'class': 'LeveledCompactionStrategy'}
  AND gc_grace_seconds = 0
  AND compression = {'sstable_compression' : 'LZ4Compressor'};
