package com.bazaarvoice.emodb.sor.api;

import java.util.List;
import java.util.Map;

/**
 * Defines the interface for storing and retrieving the stash start run timestamps.
 * <p/>
 * It's used in stash process (to update the start timestamps) and in compaction (to delay the deletion of deltas).
 */
public interface CompactionControlSource {

    void updateStashTime(String id, long timestamp, List<String> placements, long expiredTimestamp, String dataCenter);

    void deleteStashTime(String id, String dataCenter);

    StashRunTimeInfo getStashTime(String id, String dataCenter);

    Map<String, StashRunTimeInfo> getAllStashTimes();

    Map<String, StashRunTimeInfo> getStashTimesForPlacement(String placement);
}