package com.bazaarvoice.emodb.web.compactioncontrol;

import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.api.StashRunTimeInfo;
import com.bazaarvoice.emodb.sor.api.StashTimeKey;
import com.bazaarvoice.emodb.sor.compactioncontrol.LocalCompactionControl;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/*
 * This delegate implementation
 * - iterates over all the datacenter sources for update and delete operations.
 * - queries only the local datacenter for reads.
 */
public class DelegateCompactionControlSource implements CompactionControlSource {

    private static final Logger _log = LoggerFactory.getLogger(DelegateCompactionControlSource.class);

    private Provider<List<CompactionControlSource>> _compactionControlSourceListProvider;
    private CompactionControlSource _localCompactionControl;

    @Inject
    public DelegateCompactionControlSource(@AllCompactionControlSources Provider<List<CompactionControlSource>> compactionControlSourceListProvider,
                                           @LocalCompactionControl CompactionControlSource localCompactionSource) {
        _compactionControlSourceListProvider = checkNotNull(compactionControlSourceListProvider, "compactionControlSourceListProvider");
        _localCompactionControl = checkNotNull(localCompactionSource, "localCompactionSource");
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
            return _localCompactionControl.getStashTime(id, dataCenter);
        } catch (Exception e) {
            _log.error("Failed to get stash timestamp info for id: {}", id, e);
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Map<StashTimeKey, StashRunTimeInfo> getAllStashTimes() {
        try {
            return _localCompactionControl.getAllStashTimes();
        } catch (Exception e) {
            _log.error("Failed to get all stash timestamps info", e);
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Map<StashTimeKey, StashRunTimeInfo> getStashTimesForPlacement(String placement) {
        try {
            return _localCompactionControl.getStashTimesForPlacement(placement);
        } catch (Exception e) {
            _log.error("Failed to get all stash timestamps info for placement: {}", placement, e);
            throw Throwables.propagate(e);
        }
    }
}