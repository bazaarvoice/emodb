package com.bazaarvoice.emodb.sor.compactioncontrol;

import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.api.StashRunTimeInfo;
import com.bazaarvoice.emodb.sor.api.StashTimeKey;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/* For Testing Purpose only */
public class InMemoryCompactionControlSource implements CompactionControlSource {

    private static final Logger _log = LoggerFactory.getLogger(InMemoryCompactionControlSource.class);

    private Map<String, StashRunTimeInfo> _stashStartTimestampInfo = Maps.newConcurrentMap();

    @Override
    public void updateStashTime(String id, long timestamp, List<String> placements, long expiredTimestamp, String dataCenter) {
        requireNonNull(id, "id");
        requireNonNull(placements, "placements");
        requireNonNull(dataCenter, "dataCenter");

        try {
            _stashStartTimestampInfo.put(zkKey(id, dataCenter), new StashRunTimeInfo(timestamp, placements, dataCenter, expiredTimestamp));
        } catch (Exception e) {
            _log.error("Failed to update stash timestamp info for id: {}", id, e);
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteStashTime(String id, String dataCenter) {
        requireNonNull(id, "id");

        try {
            _stashStartTimestampInfo.remove(zkKey(id, dataCenter));
        } catch (Exception e) {
            _log.error("Failed to delete stash timestamp info for id: {}", id, e);
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public StashRunTimeInfo getStashTime(String id, String dataCenter) {
        requireNonNull(id, "id");

        return _stashStartTimestampInfo.get(zkKey(id, dataCenter));
    }

    @Override
    public Map<StashTimeKey, StashRunTimeInfo> getAllStashTimes() {
        return DefaultCompactionControlSource.getStashTimesWithTupleKeys(_stashStartTimestampInfo);
    }

    @Override
    public Map<StashTimeKey, StashRunTimeInfo> getStashTimesForPlacement(String placement) {
        return DefaultCompactionControlSource.getStashTimesWithTupleKeys(
                _stashStartTimestampInfo.size() > 0 ? _stashStartTimestampInfo.entrySet()
                        .stream()
                        .filter(stashTime -> stashTime.getValue().getPlacements().contains(placement))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                        : ImmutableMap.of());
    }

    private String zkKey(String id, String dataCenter) {
        return DefaultCompactionControlSource.zkKey(id, dataCenter);
    }
}