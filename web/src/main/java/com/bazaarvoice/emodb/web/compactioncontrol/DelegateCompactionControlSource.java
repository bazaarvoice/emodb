package com.bazaarvoice.emodb.web.compactioncontrol;

import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.api.StashRunTimeInfo;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class DelegateCompactionControlSource implements CompactionControlSource {

    private static final Logger _log = LoggerFactory.getLogger(DelegateCompactionControlSource.class);

    private Provider<List<CompactionControlSource>> _compactionControlSourceListProvider;

    @Inject
    public DelegateCompactionControlSource(@AllCompactionControlSources Provider<List<CompactionControlSource>> compactionControlSourceListProvider) {
        _compactionControlSourceListProvider = checkNotNull(compactionControlSourceListProvider, "compactionControlSourceListProvider");
    }

    @Override
    public void updateStashTime(String id, long timestamp, List<String> placements, long expiredTimestamp, String datacenter) {
        try {
            for (CompactionControlSource compactionControlSource : _compactionControlSourceListProvider.get()) {
                compactionControlSource.updateStashTime(id, timestamp, placements, expiredTimestamp, datacenter);
            }
        } catch (Exception e) {
            _log.error("Failed to update stash timestamp info for id: {}", id, e);
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void deleteStashTime(String id, String dataCenter) {
        try {
            for (CompactionControlSource compactionControlSource : _compactionControlSourceListProvider.get()) {
                compactionControlSource.deleteStashTime(id, dataCenter);
            }
        } catch (Exception e) {
            _log.error("Failed to delete stash timestamp info for id: {}", id, e);
            throw Throwables.propagate(e);
        }
    }

    @Override
    public StashRunTimeInfo getStashTime(String id, String dataCenter) {
        try {
            for (CompactionControlSource compactionControlSource : _compactionControlSourceListProvider.get()) {
                StashRunTimeInfo stashRunTimeInfo = compactionControlSource.getStashTime(id, dataCenter);
                if (stashRunTimeInfo != null) {
                    return stashRunTimeInfo;
                }
            }
        } catch (Exception e) {
            _log.error("Failed to get stash timestamp info for id: {}", id, e);
            throw Throwables.propagate(e);
        }
        return null;
    }

    @Override
    public Map<String, StashRunTimeInfo> getAllStashTimes() {
        Map<String, StashRunTimeInfo> stashTimes = Maps.newLinkedHashMap();
        try {
            for (CompactionControlSource compactionControlSource : _compactionControlSourceListProvider.get()) {
                stashTimes.putAll(compactionControlSource.getAllStashTimes());
            }
        } catch (Exception e) {
            _log.error("Failed to get all stash timestamps info", e);
            throw Throwables.propagate(e);
        }
        return stashTimes;
    }

    @Override
    public Map<String, StashRunTimeInfo> getStashTimesForPlacement(String placement) {
        Map<String, StashRunTimeInfo> stashTimes = Maps.newLinkedHashMap();
        try {
            for (CompactionControlSource compactionControlSource : _compactionControlSourceListProvider.get()) {
                stashTimes.putAll(compactionControlSource.getStashTimesForPlacement(placement));
            }
        } catch (Exception e) {
            _log.error("Failed to get all stash timestamps info for placement: {}", placement, e);
            throw Throwables.propagate(e);
        }
        return stashTimes;
    }
}
