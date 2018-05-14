package com.bazaarvoice.emodb.sor.compactioncontrol;

import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.api.StashRunTimeInfo;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/* For Testing Purpose only */
public class InMemoryCompactionControlSource implements CompactionControlSource {

    private static final Logger _log = LoggerFactory.getLogger(InMemoryCompactionControlSource.class);

    private Map<String, StashRunTimeInfo> _stashStartTimestampInfo = Maps.newConcurrentMap();

    @Override
    public void updateStashTime(String id, long timestamp, List<String> placements, long expiredTimestamp, String dataCenter) {
        checkNotNull(id, "id");
        checkNotNull(placements, "placements");
        checkNotNull(dataCenter, "dataCenter");

        try {
            _stashStartTimestampInfo.put(id, new StashRunTimeInfo(timestamp, placements, dataCenter, expiredTimestamp));
        } catch (Exception e) {
            _log.error("Failed to update stash timestamp info for id: {}", id, e);
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void deleteStashTime(String id, String dataCenter) {
        checkNotNull(id, "id");

        try {
            _stashStartTimestampInfo.remove(id);
        } catch (Exception e) {
            _log.error("Failed to delete stash timestamp info for id: {}", id, e);
            throw Throwables.propagate(e);
        }
    }

    @Override
    public StashRunTimeInfo getStashTime(String id, String dataCenter) {
        checkNotNull(id, "id");

        return _stashStartTimestampInfo.get(id);
    }

    @Override
    public Map<String, StashRunTimeInfo> getAllStashTimes() {
        return _stashStartTimestampInfo;
    }

    @Override
    public Map<String, StashRunTimeInfo> getStashTimesForPlacement(String placement) {
        return _stashStartTimestampInfo.size() > 0 ? _stashStartTimestampInfo.entrySet()
                .stream()
                .filter(stashTime -> stashTime.getValue().getPlacements().contains(placement))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                : ImmutableMap.of();
    }
}