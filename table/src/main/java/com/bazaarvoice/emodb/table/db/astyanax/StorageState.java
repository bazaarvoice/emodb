package com.bazaarvoice.emodb.table.db.astyanax;

import java.time.Instant;

import static com.bazaarvoice.emodb.table.db.astyanax.JsonMap.Attribute;
import static com.bazaarvoice.emodb.table.db.astyanax.JsonMap.TimestampAttribute;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Life cycle states of {@link Storage} objects.
 * <p>
 * For the most part, storages are created in the PRIMARY state and used as the main data store for a table master
 * or facade.  The life cycle of a regular storage from create through drop looks like this:
 * <pre>
 *  Action            Storage State    Maintenance Type
 *  ---------------   --------------   ---------------------
 *  Create table      PRIMARY          Metadata (system dc)
 *  Drop table        DROPPED          Metadata (system dc)
 *  Purge data (1)    PURGED_1         Data (placement dc)
 *  Purge data (2)    PURGED_2         Data (placement dc)
 *  Final delete      -                Metadata (system dc)
 * </pre>
 * <p>
 * Mirror storages are only used to implement moving a storage from one placement/#shards to another.  The life cycle
 * of a move proceeds something like this:
 * <pre>
 *  Action                  Source State        Destination State    Maintenance Type
 *  ---------------------   -----------------   ------------------   ---------------------
 *  Create table            PRIMARY             -                    Metadata (system dc)
 *  Move table              PRIMARY             MIRROR_CREATED       Metadata (system dc)
 *  Activate mirror         PRIMARY             MIRROR_ACTIVATED     Metadata (system dc)
 *  Copy data src->dest     PRIMARY             MIRROR_COPIED        Data (placement dc)
 *  Wait for replication    PRIMARY             MIRROR_CONSISTENT    Data (placement dc)
 *  Promote mirror          MIRROR_DEMOTED      PROMOTED             Metadata (system dc)
 *  Verify promotion        MIRROR_DEMOTED      PRIMARY              Metadata (system dc)
 *  Mark old expired        MIRROR_EXPIRING     PRIMARY              Metadata (system dc)
 *  Expire old storage      MIRROR_EXPIRED      PRIMARY              Metadata (system dc)
 *  Drop old storage        DROPPED             PRIMARY              Metadata (system dc)
 *  Purge data (1)          PURGED_1            PRIMARY              Data (placement dc)
 *  Purge data (2)          PURGED_2            PRIMARY              Data (placement dc)
 *  Final delete            -                   PRIMARY              Metadata (system dc)
 * </pre>
 * The sequence of states was designed to work correctly with an eventually consistent data store where (a) data writes
 * may take time to replicate to all data centers and (b) metadata writes may partially succeed--a quorum write may
 * succeed on one node but not enough nodes to reach quorum, such that later repair may cause that failed write to
 * become visible to readers.  The latter is the most difficult to handle correctly, and requires (as much as practical)
 * that every state transition may be re-tried idempotently.
 * <p>
 * Embrace: writers write what they know, readers read all the data and sort out what actually happened.  For example,
 * a move may attempt to promote mirror A to be primary by writing a 'promotionId' indicating that mirror A should
 * become the primary.  But that write may partially fail such that one Cassandra node contains the promotionId but
 * the others don't.  Then, later, a subsequent move could attempt to promote mirror B to be primary.  If a Cassandra
 * repair causes both promotionIds to become visible to all readers, it is now the responsibility of the reader to
 * sort out deterministically which promotion wins.  In this implementation the promotionId values are TimeUUIDs and
 * readers are implemented to choose the most recent TimeUUID, so last promote wins.
 * <p>
 * For steps that reconfigure readers and writers (switch readers from one storage to another or enable/disable
 * write mirroring to a particular mirror) there is always a two-step dance that ensures all servers have applied
 * the new configuration before moving on to the next step:
 * <ul>
 * <li>
 *     Mirror creation moves from "-" to MIRROR_CREATED to MIRROR_ACTIVATED in one maintenance operation.
 *     <p>
 *     A mirror is created in the MIRROR_CREATED state that enables write mirroring, then written with GLOBAL
 *     (EACH_QUORUM) consistency, and a cache flush is sent to every server in every data center.  If the write fails
 *     (or only partially succeeds) or the cache flush fails then the mirror is left in the MIRROR_CREATED step.
 *     Subsequent maintenance will idempotently re-create the mirror and only then move the mirror to MIRROR_ACTIVATED
 *     if the write and cache flush succeed.
 * </li>
 * <li>
 *     Mirror creation moves from MIRROR_CONSISTENT to PROMOTED to PRIMARY in one maintenance operation.
 *     <p>
 *     Promoting a mirror writes a 'promotionId' that moves it to the PROMOTED state which causes readers to switch
 *     from the old storage to the new storage.  The promotionId is written with GLOBAL consistency and a cache flush
 *     is sent to every server in the data center.  If the write or cache flush fails then the storage is left in the
 *     PROMOTED state.  Subsequent maintenance will idempotently re-promote the storage (re-write promotionId) and only
 *     then move the mirror to PRIMARY if the write and cache flush succeed.
 * </li>
 * <li>
 *     Mirror expiration moves from MIRROR_EXPIRING to MIRROR_EXPIRED to DROPPED in one maintenance operation.
 *     <p>
 *     Expiring a mirror requires that all readers stop supporting reads on a mirror before writers stop writing to
 *     the mirror.  It would be an error if a read operation returned successfully even though it read stale data
 *     written before the read started, where the data was stale because a server had turned off write mirroring early.
 *     So expiring a mirror writes 'mirrorExpiredAt' and moves the mirror into the MIRROR_EXPIRED state.  If the write
 *     or cache flush fails then the storage is left in the MIRROR_EXPIRED state.  Subsequent maintenance will
 *     idempotently re-expire the storage (re-write mirrorExpiredAt) and only then move the mirror to DROPPED if the
 *     write and cache flush succeed.
 * </li>
 * </ul>
 */
enum StorageState {
    /** Newly created mirror (future move destination), likely has no data, it's possible not all servers know about it. */
    MIRROR_CREATED("mirrorCreatedAt"),

    /** Mirror is empty or has partial content, all servers are mirroring writes to the mirror. */
    MIRROR_ACTIVATED("mirrorActivatedAt"),

    /** Mirror has all content, matches the primary in the data center in which the copy was performed. */
    MIRROR_COPIED("mirrorCopiedAt"),

    /** Mirror has all content, data copy has replicated to all data centers. */
    MIRROR_CONSISTENT("mirrorConsistentAt"),

    /** Promoted to primary, but it's possible not all servers know about the switch yet. */
    PROMOTED(Storage.PROMOTION_ID/*this is a time uuid, not a regular transition timestamp attribute*/),

    /** Live primary storage for the group, not a mirror. */
    PRIMARY("primaryAt"/*transition timestamp is missing when storage starts out as primary*/),

    /** No longer primary, but it's possible not all servers know about the switch yet. */
    MIRROR_DEMOTED(/*no transition attributes--it's the primary that changes*/) {
        @Override
        Instant getTransitionedAt(Storage storage) {
            return storage.getPrimary().getTransitionedTimestamp(PRIMARY);  // Might be null since 'primaryAt' isn't always present.
        }
    },

    /** Mirror has all content, reads still allowed, writes still mirrored, expiration scheduled. */
    MIRROR_EXPIRING(Storage.MIRROR_EXPIRES_AT /*no transition timestamp attribute*/),

    /** Mirror has all content, reads should be disabled (but maybe not all servers know yet), writes still mirrored. */
    MIRROR_EXPIRED("mirrorExpiredAt"/*marker may be missing when a mirror is abandoned by canceling a move*/),

    /** Reads and writes are disabled, purge is imminent. */
    DROPPED("droppedAt"),

    /** The initial pass at deleting all data in the storage is complete. */
    PURGED_1("purgedAt1"),

    /** The final pass at deleting all data in the storage is complete. */
    PURGED_2("purgedAt2"),
    ;

    private final Attribute<?> _transitionMarker;
    private final Attribute<Instant> _transitionTimestamp;

    StorageState() {
        _transitionMarker = _transitionTimestamp = null;
    }

    StorageState(String transitionTimestamp) {
        _transitionMarker = _transitionTimestamp = TimestampAttribute.create(transitionTimestamp);
    }

    StorageState(Attribute<?> transitionMarker) {
        _transitionMarker = transitionMarker;
        _transitionTimestamp = null;  // The marker came from elsewhere, don't assume it's a transition timestamp.
    }

    Attribute<?> getMarkerAttribute() {
        return checkNotNull(_transitionMarker, name());
    }

    boolean hasTransitioned(Storage storage) {
        return checkNotNull(_transitionMarker, name()).containsKey(storage.getRawJson());
    }

    Instant getTransitionedAt(Storage storage) {
        return checkNotNull(_transitionTimestamp, name()).get(storage.getRawJson());
    }

    static StorageState getState(Storage storage) {
        if (storage.isDropped()) {
            // Anything not belonging to a group is, by definition, dropped/expired.
            return pickState(storage,
                    MIRROR_EXPIRED,
                    DROPPED,
                    PURGED_1,
                    PURGED_2);

        } else if (storage.isPrimary()) {
            // Primary storage (the common case).
            if (!storage.hasTransitioned(PROMOTED)) {
                return PRIMARY; // Never was a mirror (started life as a primary master or facade).
            } else {
                return pickState(storage,
                        PROMOTED,
                        PRIMARY);
            }

        } else if (storage.getPrimary().getMoveTo() == storage) {
            // Mirror that is the eventual destination of a move.
            if (storage.isConsistent()) {
                // Original master or facade that is being resurrected by canceling/reversing a move.
                return MIRROR_CONSISTENT;
            } else {
                // Regular mirror.
                return pickState(storage,
                        MIRROR_CREATED,
                        MIRROR_ACTIVATED,
                        MIRROR_COPIED,
                        MIRROR_CONSISTENT);
            }
        } else {
            if (storage.isConsistent()) {
                // Mirror that's not in use anymore but might have been primary at one time.  Support reads for a while
                // (honor getSplit calls with split identifiers referencing the mirror) then expire and drop the mirror.
                return pickState(storage,
                        MIRROR_DEMOTED,
                        MIRROR_EXPIRING,
                        MIRROR_EXPIRED);

            } else {
                // Mirror that was abandoned before it could have been promoted.  Since it was never read,
                // go straight to the expired step.
                return MIRROR_EXPIRED;
            }
        }
    }

    private static StorageState pickState(Storage storage, StorageState... sequence) {
        for (int i = sequence.length - 1; i > 0; i--) {
            if (storage.hasTransitioned(sequence[i])) {
                return sequence[i];
            }
        }
        return sequence[0];
    }
}
