package com.bazaarvoice.emodb.table.db.astyanax;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.UnknownFacadeException;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.delta.MapDeltaBuilder;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static java.lang.String.format;

/** Internal table metadata wrapper for table json persistence format. */
class TableJson extends JsonMap {
    private static final Logger _log = LoggerFactory.getLogger(TableJson.class);

    /** Top-level table uuid for the primary data for this table.  This is a hex-encoded unsigned long. */
    private static final Attribute<String> UUID_ATTR = Attribute.create("uuid");

    /** Top-level map of json attributes that apply to every record in the table, aka the table template. */
    private static final Attribute<Map<String, Object>> ATTRIBUTES = Attribute.create("attributes");

    /** Top-level map of uuid->storage-related properties, one for each table uuid associated with this table. */
    private static final Attribute<Map<String, Map<String, Object>>> STORAGE = Attribute.create("storage");

    private final Storage _master;
    private final List<Storage> _facades;
    private final List<Storage> _storages;

    TableJson(Map<String, Object> json) {
        super(json);

        // Get the uuid of the master primary storage.
        String masterUuid = get(UUID_ATTR);

        // Create 'Storage' objects that wrap all the json maps describing where data for this table lives.
        List<Storage> storages = Lists.newArrayList();
        Map<String, Map<String, Object>> storageMap = get(STORAGE);
        if (storageMap != null) {
            for (Map.Entry<String, Map<String, Object>> entry : storageMap.entrySet()) {
                storages.add(new Storage(entry.getKey(), entry.getValue(), entry.getKey().equals(masterUuid)));
            }
        }
        _storages = storages;

        // Loop through the storage objects and organize the data into master, facades, expired, pick primaries.
        // This is the essence of the storage life cycle state machine w/an eventually consistent data store:
        // writers write the state markers they know, readers read them and sort out what actually happened.
        Storage master = null;
        List<Storage> facades = Lists.newArrayList();
        // Bucket the active storage entries into groups, one group for the master and one for each facade.
        ListMultimap<String, Storage> groupMap = ArrayListMultimap.create();
        for (Storage storage : storages) {
            if (!storage.hasTransitioned(StorageState.DROPPED)) {
                groupMap.put(storage.getGroupId(), storage);
            }
        }
        // Pick the primary storage in each group.  The rest are mirrors.
        Set<String> facadePlacements = Sets.newHashSet();
        for (Collection<Storage> group : groupMap.asMap().values()) {
            Storage primary = Storage.initializeGroup(group);
            if (primary == null) {
                continue;
            }
            if (!primary.isFacade()) {
                // Master data
                if (!primary.getUuidString().equals(masterUuid)) {
                    // Looks like there are multiple primaries for the master.  We always pick the one that matches
                    // masterUuid, but we should log an error because this isn't supposed to happen.  It is possible,
                    // however, when quorum is broken--multiple Cassandra servers are lost and restored and data is
                    // not repaired correctly.
                    _log.error("Table {} has an orphaned master storage (uuid={}), please verify data integrity.",
                            getTable(), primary.getUuidString());
                    continue;
                }
                master = primary;
            } else {
                // Facade data
                if (!facadePlacements.add(primary.getPlacement())) {
                    // Multiple facades for the same placement is not supposed to happen, log an error.
                    _log.error("Table {} has multiple facades for the same placement {} (uuid={}), please verify data integrity.",
                            getTable(), primary.getPlacement(), primary.getUuidString());
                    continue;
                }
                facades.add(primary);
            }
        }
        _master = master;
        _facades = facades;
    }

    boolean isDeleted() {
        return Intrinsic.isDeleted(getRawJson());
    }

    boolean isDropped() {
        return _master == null;
    }

    String getTable() {
        return Intrinsic.getId(getRawJson());
    }

    /** Returns the uuid of the primary storage for the master data store. */
    String getUuidString() {
        return get(UUID_ATTR);
    }

    Map<String, Object> getAttributeMap() {
        return get(ATTRIBUTES);
    }

    Collection<Storage> getStorages() {
        return _storages;
    }

    Storage getMasterStorage() {
        return _master;
    }

    Collection<Storage> getFacades() {
        return _facades;
    }

    Storage getFacadeForPlacement(String placement) {
        for (Storage facade : _facades) {
            if (placement.equals(facade.getPlacement())) {
                return facade;
            }
        }
        throw new UnknownFacadeException(format("Unknown facade: %s in %s", getTable(), placement), getTable());
    }

    //
    // Update methods, via SoR Delta objects.
    //

    static Delta newCreateTable(String uuid, Map<String, ?> attributes, String placement, int shardsLog2) {
        // Don't overwrite information about dropped tables that may need to be cleaned up (under the 'storage' key).
        return Deltas.mapBuilder()
                .put(UUID_ATTR.key(), uuid)
                .put(ATTRIBUTES.key(), attributes)
                .update(STORAGE.key(), Deltas.mapBuilder()
                        .put(uuid, storageAttributesBuilder(placement, shardsLog2, false).build())
                        .build())
                .removeRest()
                .build();
    }

    static Delta newCreateFacade(String uuid, String placement, int shardsLog2) {
        return Deltas.mapBuilder()
                .update(STORAGE.key(), Deltas.mapBuilder()
                        .put(uuid, storageAttributesBuilder(placement, shardsLog2, true).build())
                        .build())
                .build();
    }

    /** Mark an entire table as dropped.  A maintenance job will come along later and actually purge the data. */
    Delta newDropTable() {
        Delta storageDelta = Deltas.mapBuilder()
                .put(StorageState.DROPPED.getMarkerAttribute().key(), now())
                .build();

        MapDeltaBuilder storageMapDelta = Deltas.mapBuilder();
        if (_master != null) {
            for (Storage storage : _master.getPrimaryAndMirrors()) {
                storageMapDelta.update(storage.getUuidString(), storageDelta);
            }
        }
        for (Storage facade : _facades) {
            for (Storage storage : facade.getPrimaryAndMirrors()) {
                storageMapDelta.update(storage.getUuidString(), storageDelta);
            }
        }

        // Delete most of the information about the table, but leave enough that we can run a job later that
        // purges all the data out of Cassandra.
        return Deltas.mapBuilder()
                .remove(UUID_ATTR.key())
                .remove(ATTRIBUTES.key())
                .update(STORAGE.key(), storageMapDelta.build())
                .removeRest()
                .build();
    }

    /** Mark a facade as dropped.  A maintenance job will come along later and actually purge the data. */
    Delta newDropFacade(Storage facade) {
        Delta storageDelta = Deltas.mapBuilder()
                .put(StorageState.DROPPED.getMarkerAttribute().key(), now())
                .build();

        MapDeltaBuilder storageMapDelta = Deltas.mapBuilder();
        for (Storage storage : facade.getPrimaryAndMirrors()) {
            storageMapDelta.update(storage.getUuidString(), storageDelta);
        }

        return Deltas.mapBuilder()
                .update(STORAGE.key(), storageMapDelta.build())
                .build();
    }

    /** Final delete of table and/or storage metadata after all data has been purged for the last time. */
    Delta newDeleteStorage(Storage storage) {
        return Deltas.mapBuilder()
                .update(STORAGE.key(), Deltas.mapBuilder()
                        .remove(storage.getUuidString())
                        .deleteIfEmpty()
                        .build())
                .deleteIfEmpty()
                .build();
    }

    Delta newSetAttributes(Map<String, ?> attributes) {
        return Deltas.mapBuilder()
                .put(ATTRIBUTES.key(), attributes)
                .build();
    }

    /** First step in a move, creates the destination storage and sets up mirroring. */
    Delta newMoveStart(Storage src, String destUuid, String destPlacement, int destShardsLog2) {
        return Deltas.mapBuilder()
                .update(STORAGE.key(), Deltas.mapBuilder()
                        .update(src.getUuidString(), Deltas.mapBuilder()
                                .put(Storage.MOVE_TO.key(), destUuid)
                                .build())
                        .put(destUuid, storageAttributesBuilder(destPlacement, destShardsLog2, src.isFacade())
                                .put(StorageState.MIRROR_CREATED.getMarkerAttribute().key(), now())
                                .put(Storage.GROUP_ID.key(), src.getGroupId())
                                .build())
                        .build())
                .build();
    }

    /** First step in a move placement. Same as newMoveStart, except it doesn't run purge for faster results */
    Delta newMovePlacementStart(Storage src, String destUuid, String destPlacement, int destShardsLog2) {
        return Deltas.mapBuilder()
                .update(STORAGE.key(), Deltas.mapBuilder()
                        .update(src.getUuidString(), Deltas.mapBuilder()
                                .put(Storage.MOVE_TO.key(), destUuid)
                                .put(Storage.IS_PLACEMENT_MOVE.key(), true)
                                .build())
                        .put(destUuid, storageAttributesBuilder(destPlacement, destShardsLog2, src.isFacade())
                                .put(StorageState.MIRROR_CREATED.getMarkerAttribute().key(), now())
                                .put(Storage.GROUP_ID.key(), src.getGroupId())
                                .put(Storage.IS_PLACEMENT_MOVE.key(), true)
                                .build())
                        .build())
                .build();
    }

    /** Start a move to an existing mirror that may or may not have once been primary. */
    Delta newMoveRestart(Storage src, Storage dest) {
        // If the destination used to be the initial primary for the group, it's consistent and ready to promote
        // but lacking the MIRROR_CONSISTENT marker attribute which is the prereq for promotion.  Add one.
        Delta consistentMarker = dest.isConsistent() ?
                Deltas.conditional(Conditions.isUndefined(), Deltas.literal(now())) :
                Deltas.noop();
        return Deltas.mapBuilder()
                .update(STORAGE.key(), Deltas.mapBuilder()
                        .update(src.getUuidString(), Deltas.mapBuilder()
                                .put(Storage.MOVE_TO.key(), dest.getUuidString())
                                .build())
                        .update(dest.getUuidString(), Deltas.mapBuilder()
                                // Clean slate: clear 'moveTo' plus all markers for promotion states and later.
                                .update(StorageState.MIRROR_CONSISTENT.getMarkerAttribute().key(), consistentMarker)
                                .remove(Storage.MOVE_TO.key())
                                .remove(Storage.PROMOTION_ID.key())
                                .remove(StorageState.PRIMARY.getMarkerAttribute().key())
                                .remove(StorageState.MIRROR_EXPIRING.getMarkerAttribute().key())
                                .remove(StorageState.MIRROR_EXPIRED.getMarkerAttribute().key())
                                .build())
                        .build())
                .build();
    }

    Delta newMovePromoteMirror(Storage mirror) {
        UUID promotionId = TimeUUIDs.newUUID();
        // Use conditional deltas to avoid accidentally resurrecting storage objects.
        MapDeltaBuilder mapDelta = Deltas.mapBuilder();
        mapDelta.updateIfExists(STORAGE.key(), Deltas.mapBuilder()
                .updateIfExists(mirror.getUuidString(), Deltas.mapBuilder()
                        // Set 'promotionId' to take precedence over other primary candidates.  To ensure a clean
                        // slate (just a precaution--it shouldn't be necessary), remove 'moveTo' plus markers for
                        // non-dropped states after 'promotionId'.
                        .put(Storage.PROMOTION_ID.key(), promotionId.toString())
                        .remove(Storage.MOVE_TO.key())
                        .remove(StorageState.PRIMARY.getMarkerAttribute().key())
                        .remove(StorageState.MIRROR_EXPIRING.getMarkerAttribute().key())
                        .remove(StorageState.MIRROR_EXPIRED.getMarkerAttribute().key())
                        .build())
                .build());
        if (!mirror.isFacade()) {
            mapDelta.updateIfExists(UUID_ATTR.key(), Deltas.literal(mirror.getUuidString()));
        }
        return mapDelta.build();
    }

    Delta newMoveCancel(Storage primary) {
        return Deltas.mapBuilder()
                .updateIfExists(STORAGE.key(), Deltas.mapBuilder()
                        .updateIfExists(primary.getUuidString(), Deltas.mapBuilder()
                                .remove(Storage.MOVE_TO.key())
                                .build())
                        .build())
                .build();
    }

    Delta newNextState(String storageUuid, StorageState state, Object markerValue) {
        if (markerValue instanceof DateTime) {
            markerValue = TimestampAttribute.format((DateTime) markerValue);
        }
        // Uses conditional deltas to avoid accidentally creating/resurrecting storage objects.
        return Deltas.mapBuilder()
                .updateIfExists(STORAGE.key(), Deltas.mapBuilder()
                        .updateIfExists(storageUuid, Deltas.mapBuilder()
                                .put(state.getMarkerAttribute().key(), markerValue)
                                .build())
                        .build())
                .build();
    }

    private static ImmutableMap.Builder<String, Object> storageAttributesBuilder(String placement,
                                                                                 int shardsLog2,
                                                                                 boolean facade) {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        builder.put(Storage.PLACEMENT.key(), placement);
        builder.put(Storage.SHARDS.key(), 1 << shardsLog2);
        if (facade) {
            builder.put(Storage.FACADE.key(), true);
        }
        return builder;
    }

    private static String now() {
        return JsonHelper.formatTimestamp(new Date());
    }
}
