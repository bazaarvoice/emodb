USE ${keyspace};

-- A single wide row, one column for each subscription.  The subscription data is JSON metadata about the subscription.
-- The app always loads all subscription data into memory at once, and a single row makes that fast & efficient.
-- This isn't a high-traffic CF so it shouldn't be necessary to distribute the data around the Cassandra ring.
CREATE TABLE IF NOT EXISTS subscription (
    rowkey blob,
    subscriptionid text,
    subscription text,
    PRIMARY KEY (rowkey, subscriptionid)
) WITH COMPACT STORAGE
  AND compression = {'sstable_compression' : 'LZ4Compressor'};

-- One row per subscription, each row contains a list of slabIds that reference rows in the 'slab' CF.
-- Slabs with "open=true" may still be in the process of being written to.  Closed slabs will never grow, just shrink
-- as events get processed and deleted from the slabs.
-- Zero gc_grace_seconds to disable tombstones to improve read performance.  If a delete gets lost because of the lack
-- of tombstones, the application will notice and simply delete the row again.
CREATE TABLE IF NOT EXISTS manifest (
  subscription text,
  slabid timeuuid,
  open boolean,
  PRIMARY KEY (subscription, slabid)
) WITH COMPACT STORAGE
  AND gc_grace_seconds = 0
  AND compaction={'sstable_size_in_mb': '40', 'tombstone_compaction_interval': '14400', 'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}
  AND compression = {'sstable_compression': ''};

-- One row per slabId.  Each "slab" contains a list of events, one event per column.  The application imposes a maximum
-- number of events per slab, typically 1000, to avoid getting wide rows with lots of tombstones that hurt read
-- performance.  Low gc_grace_seconds to improve read performance, but not as low as with the 'manifest' CF.  If a
-- delete gets lost, the Databus will issue a duplicate event which hurts the performance of Databus consumers.
CREATE TABLE IF NOT EXISTS slab (
  slabid timeuuid,
  eventidx int,
  event blob,
  PRIMARY KEY (slabid, eventidx)
) WITH COMPACT STORAGE
  AND compaction={'sstable_size_in_mb': '160', 'class': 'LeveledCompactionStrategy'}
  AND gc_grace_seconds = 1200
  AND compression = {'sstable_compression' : 'LZ4Compressor'};


-- Dedup Event Store's PersistentSortedQueue

-- One row per queue.  One column for each segment containing actual queue data.  The Java code recycles column uuids
-- as segments are added and deleted so, in general, tombstones shouldn't accumulate and aren't a major concern.
CREATE TABLE IF NOT EXISTS dedup_md (
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
CREATE TABLE IF NOT EXISTS dedup_data (
  dataid timeuuid,
  event blob,
  unused blob,
  PRIMARY KEY (dataid, event)
) WITH COMPACT STORAGE
  AND compaction={'sstable_size_in_mb': '160', 'class': 'LeveledCompactionStrategy'}
  AND gc_grace_seconds = 0
  AND compression = {'sstable_compression' : 'LZ4Compressor'};
