package com.bazaarvoice.emodb.sor.compactioncontrol;

import com.bazaarvoice.emodb.common.zookeeper.store.MapStore;
import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.api.StashRunTimeInfo;
import com.bazaarvoice.emodb.sor.api.StashTimeKey;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/*
 * Default implementation which uses ZooKeeper to store stash times just in the local data center.
 *
 */
public class DefaultCompactionControlSource implements CompactionControlSource {

    private static final Logger _log = LoggerFactory.getLogger(DefaultCompactionControlSource.class);

    private final MapStore<StashRunTimeInfo> _stashStartTimestampInfo;

    @Inject
    public DefaultCompactionControlSource(@StashRunTimeMapStore final MapStore<StashRunTimeInfo> stashStartTimestampInfo, final MetricRegistry metricRegistry) {
        _stashStartTimestampInfo = requireNonNull(stashStartTimestampInfo, "stashStartTimestampInfo");
        requireNonNull(metricRegistry, "metricRegistry").register(MetricRegistry.name("bv.emodb.scan", "CompactionControlSource", "map-size"),
                (Gauge<Integer>) () -> _stashStartTimestampInfo.keySet().size());
    }

    @Override
    public void updateStashTime(String id, long timestamp, List<String> placements, long expiredTimestamp, String dataCenter) {
        requireNonNull(id, "id");
        requireNonNull(placements, "placements");
        requireNonNull(dataCenter, "dataCenter");
        checkState(timestamp > System.currentTimeMillis() + Duration.ofSeconds(10).toMillis(), "specified timestamp seems to be in the past");

        try {
            _stashStartTimestampInfo.set(zkKey(id, dataCenter), new StashRunTimeInfo(timestamp, placements, dataCenter, expiredTimestamp));
        } catch (Exception e) {
            _log.error("Failed to update stash timestamp info for id: {}, datacenter: {}", id, dataCenter, e);
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteStashTime(String id, String dataCenter) {
        requireNonNull(id, "id");
        requireNonNull(dataCenter, "dataCenter");

        try {
            _stashStartTimestampInfo.remove(zkKey(id, dataCenter));
        } catch (Exception e) {
            _log.error("Failed to delete stash timestamp info for id: {}, datacenter: {}", id, dataCenter, e);
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public StashRunTimeInfo getStashTime(String id, String dataCenter) {
        requireNonNull(id, "id");
        requireNonNull(dataCenter, "dataCenter");

        return _stashStartTimestampInfo.get(zkKey(id, dataCenter));
    }

    @Override
    public Map<StashTimeKey, StashRunTimeInfo> getAllStashTimes() {
        // Zookeeper entries have "ID@datacenter" as key names; example: daily-2018-05-26-00-00-00@eu-west-1-prod.
        // Separating the ID and datacenter in the key.
        return getStashTimesWithTupleKeys(_stashStartTimestampInfo.getAll());
    }

    @Override
    public Map<StashTimeKey, StashRunTimeInfo> getStashTimesForPlacement(String placement) {
        Map<String, StashRunTimeInfo> stashTimes = _stashStartTimestampInfo.getAll();
        return getStashTimesWithTupleKeys(stashTimes.size() > 0 ? stashTimes.entrySet().stream()
                .filter(stashTime -> stashTime.getValue().getPlacements().contains(placement))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                : ImmutableMap.of());
    }

    @VisibleForTesting
    protected static String zkKey(String id, String dataCenter) {
        if (id.contains(StashTimeKey.ZK_STRING_DELIMITER)) {
          throw new IllegalArgumentException("Id cannot contain character: " + StashTimeKey.ZK_STRING_DELIMITER);
        }
        return id + StashTimeKey.ZK_STRING_DELIMITER + dataCenter;
    }

    @VisibleForTesting
    protected static Map<StashTimeKey, StashRunTimeInfo> getStashTimesWithTupleKeys(Map<String, StashRunTimeInfo> stashTimesFromZk) {
        Map<StashTimeKey, StashRunTimeInfo> allStashTimes = Maps.newHashMap();
        for (Map.Entry<String, StashRunTimeInfo> stashTimeFromZk : stashTimesFromZk.entrySet()) {
            allStashTimes.put(StashTimeKey.fromString(stashTimeFromZk.getKey()), stashTimeFromZk.getValue());
        }
        return allStashTimes;
    }
}