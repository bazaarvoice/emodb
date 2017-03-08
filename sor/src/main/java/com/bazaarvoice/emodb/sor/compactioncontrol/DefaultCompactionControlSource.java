package com.bazaarvoice.emodb.sor.compactioncontrol;

import com.bazaarvoice.emodb.common.zookeeper.store.MapStore;
import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.api.StashRunTimeInfo;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/*
 * Default implementation which uses ZooKeeper to store stash times just in the local data center.
 *
 */
public class DefaultCompactionControlSource implements CompactionControlSource {

    private static final Logger _log = LoggerFactory.getLogger(DefaultCompactionControlSource.class);

    private final MapStore<StashRunTimeInfo> _stashStartTimestampInfo;

    @Inject
    public DefaultCompactionControlSource(@StashRunTimeMapStore final MapStore<StashRunTimeInfo> stashStartTimestampInfo) {
        _stashStartTimestampInfo = checkNotNull(stashStartTimestampInfo, "stashStartTimestampInfo");
    }

    @Override
    public void updateStashTime(String id, long timestamp, List<String> placements, long expiredTimestamp, String dataCenter) {
        checkNotNull(id, "id");
        checkNotNull(placements, "placements");
        checkNotNull(dataCenter, "dataCenter");
        checkState(timestamp > System.currentTimeMillis() + Duration.ofSeconds(10).toMillis(), "specified timestamp seems to be in the past");

        try {
            _stashStartTimestampInfo.set(zkKey(id, dataCenter), new StashRunTimeInfo(timestamp, placements, dataCenter, expiredTimestamp));
        } catch (Exception e) {
            _log.error("Failed to update stash timestamp info for id: {}, datacenter: {}", id, dataCenter, e);
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void deleteStashTime(String id, String dataCenter) {
        checkNotNull(id, "id");
        checkNotNull(dataCenter, "dataCenter");

        try {
            _stashStartTimestampInfo.remove(zkKey(id, dataCenter));
        } catch (Exception e) {
            _log.error("Failed to delete stash timestamp info for id: {}, datacenter: {}", id, dataCenter, e);
            throw Throwables.propagate(e);
        }
    }

    @Override
    public StashRunTimeInfo getStashTime(String id, String dataCenter) {
        checkNotNull(id, "id");
        checkNotNull(dataCenter, "dataCenter");

        return _stashStartTimestampInfo.get(zkKey(id, dataCenter));
    }

    @Override
    public Map<String, StashRunTimeInfo> getAllStashTimes() {
        return _stashStartTimestampInfo.getAll();
    }

    @Override
    public Map<String, StashRunTimeInfo> getStashTimesForPlacement(String placement) {
        Map<String, StashRunTimeInfo> stashTimes = _stashStartTimestampInfo.getAll();
        return stashTimes.size() > 0 ? stashTimes.entrySet().stream()
                .filter(stashTime -> stashTime.getValue().getPlacements().contains(placement))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                : ImmutableMap.of();
    }

    private String zkKey(String id, String dataCenter) {
        return id + "-" + dataCenter;
    }
}