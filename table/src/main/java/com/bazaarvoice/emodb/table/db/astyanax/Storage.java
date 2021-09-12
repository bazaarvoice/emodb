package com.bazaarvoice.emodb.table.db.astyanax;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.bazaarvoice.emodb.table.db.astyanax.RowKeyUtils.LEGACY_SHARDS_LOG2;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Wrapper around the json table storage metadata for a specific table uuid.
 * <p>
 * Data for a table is stored in buckets called "storages" in the source code, where each "storage" has a
 * cluster-unique 64-bit uuid that's part of the row key prefix for every Cassandra row.
 * <p>
 * The hierarchy looks
 * something like:
 * <ol>
 * <li>
 *     A table contains one or more groups of "storage" objects.
 * </li>
 * <li>
 *     Each storage in a group is a separate copy of the same set of data.
 * <p>
 *     In the steady state, all storages in a particular group have identical copies of the data.  The only exception
 *     is when a new storage is first created and added to a group--its data set must be copied from one of the other
 *     storages before it matches the rest.
 * <p>
 *     For live (non-dropped) tables there's always one group of storages for the master table data, the data you
 *     read&write with regular operations (see {@link TableJson#getMasterStorage()}).
 * <p>
 *     There are zero or more groups of objects for facades, one group for each facade (see
 *     {@link TableJson#getFacades()}).
 * <p>
 *     Dropped storage objects don't belong to a group (see @link TableJson#getStorage()} where
 *     {@code isDropped() == true}).
 * </li>
 * <li>
 *     Within a group of storages there's always one storage that's designated the "primary".  The remainder are
 *     designated "mirrors".
 * <p>
 *     The system tries to keep the primary and the mirrors in sync by writing every write
 *     to every mirror, so they all have exactly the same data at all times (except for writes in-flight).
 * <p>
 *     Writers apply every update to the primary and to all mirrors.  In the steady state, this keeps all the
 *     storages in a group in sync so they always have the exact same data.  The only exception is immediately after
 *     a mirror is created where not all servers may know about the new mirror yet.  This is important because the
 *     system can't perform the initial background bulk-copy to seed a new mirror until it knows every server knows
 *     about the new mirror and is actively mirroring writes to it.
 * <p>
 *     In general, readers always read from the primary.  The one exception is that the getSplits() call returns split
 *     identifiers that reference specific storage uuids, so the getSplit() method is allowed to read from mirrors
 *     as long as the mirror is consistent and has not expired.
 * </li>
 * <li>
 *     Within each storage the data is sharded across a number of shards, designed to ensure that data is spread
 *     evenly across a Cassandra cluster that uses the ByteOrderedPartitioner.  See the "shards" and "shardsLog2"
 *     properties.
 * </li>
 * </ol>
 */
class Storage extends JsonMap implements Comparable<Storage> {
    /**
     * Storage-level flag, if true then the table uuid points to facade data, not the primary data.
     */
    static final Attribute<Boolean> FACADE = Attribute.create("facade");

    /**
     * Storage-level identifier of which keyspace and column family contains the storage data.
     */
    static final Attribute<String> PLACEMENT = Attribute.create("placement");

    /**
     * Storage-level # of shards the storage data is split across, always a power of 2.
     */
    static final Attribute<Integer> SHARDS = Attribute.create("shards");

    /**
     * Storage-level id identifying the group of primary+mirrors this storage belongs to.  Defaults to 'uuid'.
     */
    static final Attribute<String> GROUP_ID = Attribute.create("groupId");

    /**
     * Storage-level table uuid that data in this storage should move to.
     */
    static final Attribute<String> MOVE_TO = Attribute.create("moveTo");

    /**
     * Storage-level time uuid of when this storage was promoted from mirror to primary.
     */
    static final Attribute<String> PROMOTION_ID = Attribute.create("promotionId");

    /**
     * Storage-level timestamp of when this mirror expires and should be dropped and purged.
     */
    static final Attribute<Instant> MIRROR_EXPIRES_AT = TimestampAttribute.create("mirrorExpiresAt");

    /**
     * Storage-level marker to indicate the move is taking place as a placement level move.
     */
    static final Attribute<Boolean> IS_PLACEMENT_MOVE = Attribute.create("placementMove");

    private final String _uuid;
    private final boolean _masterPrimary;
    private List<Storage> _group;

    Storage(String uuid, Map<String, Object> json, boolean masterPrimary) {
        super(checkNotNull(json, "missing storage json for uuid: %s", uuid));
        _uuid = checkNotNull(uuid, "uuid");
        _masterPrimary = masterPrimary;
    }

    /**
     * Post-construction initialization links all non-dropped members of group together.  Returns the primary member
     * of the group, other members are mirrors.
     */
    static Storage initializeGroup(Collection<Storage> group) {
        List<Storage> sorted = Ordering.natural().immutableSortedCopy(group);
        Storage primary = sorted.get(0);  // After sorting, the first storage in each group is the primary.
        if (!primary.isConsistent()) {
            // Dropping the primary drops the entire group (primary+mirrors), so 'primary' must be a mirror that
            // should have been dropped but was missed due to error conditions related to eventual consistency.
            return null;
        }
        for (Storage storage : sorted) {
            storage._group = sorted;
        }
        return primary;
    }

    boolean isDropped() {
        // initializeGroup() doesn't set _group on deleted/dropped storage objects.  That's a more reliable signal
        // than checking 'droppedAt' since it's technically possible that droppedAt might not be set on mirrors
        // when the primary is dropped.
        return _group == null;
    }

    boolean isPrimary() {
        return this == _group.get(0);
    }

    Storage getPrimary() {
        return _group.get(0);
    }

    List<Storage> getMirrors() {
        return _group.subList(1, _group.size());
    }

    List<Storage> getPrimaryAndMirrors() {
        return _group;
    }

    // Persistent properties

    String getUuidString() {
        return _uuid;
    }

    long getUuid() {
        return TableUuidFormat.decode(_uuid);
    }

    String getGroupId() {
        return Optional.ofNullable(get(GROUP_ID)).orElse(_uuid);
    }

    boolean isFacade() {
        return Boolean.TRUE.equals(get(FACADE));
    }

    boolean isPlacementMove() {
        return Boolean.TRUE.equals(get(IS_PLACEMENT_MOVE));
    }

    String getPlacement() {
        return get(PLACEMENT);
    }

    int getShardsLog2() {
        Integer numShards = get(SHARDS);
        return (numShards != null) ? RowKeyUtils.computeShardsLog2(numShards, _uuid) : LEGACY_SHARDS_LOG2;
    }

    // State-related properties

    StorageState getState() {
        return StorageState.getState(this);
    }

    /**
     * Returns true if this storage has the marker associated with transitioning to the specified state at some
     * point.  This does not check whether the the storage is in the specified state right now.
     */
    boolean hasTransitioned(StorageState state) {
        return state.hasTransitioned(this);
    }

    /**
     * Returns the most recent time that this storage transitioned to the specified state.  This does not
     * check whether the the storage is in the specified state right now.
     */
    Instant getTransitionedTimestamp(StorageState state) {
        return state.getTransitionedAt(this);
    }

    boolean getReadsAllowed() {
        // Reads are allowed in all non-dropped states except MIRROR_EXPIRED.  Note: the only public API that
        // reads from a mirror (instead of the primary) is getSplit().
        return isConsistent() && !isMirrorExpired();
    }

    // Move-related properties

    Storage getMoveTo() {
        String destUuid = get(MOVE_TO);
        return destUuid != null ? find(getMirrors(), destUuid) : null;
    }

    Instant getMirrorExpiresAt() {
        return get(MIRROR_EXPIRES_AT);
    }

    boolean isMirrorExpired() {
        return hasTransitioned(StorageState.MIRROR_EXPIRED);
    }

    UUID getPromotionId() {
        String promotionId = get(PROMOTION_ID);
        return promotionId != null ? UUID.fromString(promotionId) : null;
    }

    boolean isConsistent() {
        // A storage might be a consistent if (a) it was originally created as a primary (never was a mirror) and
        // was therefore always consistent or (b) was created as a mirror and later achieved consistency.
        return !hasTransitioned(StorageState.MIRROR_CREATED) ||
                hasTransitioned(StorageState.MIRROR_CONSISTENT);
    }

    private Storage find(List<Storage> storages, String uuid) {
        for (Storage storage : storages) {
            if (uuid.equals(storage.getUuidString())) {
                return storage;
            }
        }
        return null;
    }

    /**
     * Storage objects sort such that primaries sort first, mirrors after.
     */
    @Override
    public int compareTo(Storage o) {
        return ComparisonChain.start()
                .compareTrueFirst(isConsistent(), o.isConsistent())  // Primaries *must* be consistent.
                .compareTrueFirst(_masterPrimary, o._masterPrimary)  // Master primary sorts first
                .compare(o.getPromotionId(), getPromotionId(), TimeUUIDs.ordering().nullsLast())  // Facade primary sorts first
                .compare(_uuid, o._uuid)  // Break ties in a way that's compatible with equals()
                .result();
    }

    /**
     * Two Storage objects are equal if they're for the same table uuid.
     */
    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof Storage && _uuid.equals(((Storage) o)._uuid));
    }

    @Override
    public int hashCode() {
        return _uuid.hashCode();
    }

    // For debugging
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("primary", isPrimary())
                .add("uuid", _uuid)
                .add("placement", getPlacement())
                .add("facade", isFacade())
                .add("state", getState())
                .toString();
    }
}
