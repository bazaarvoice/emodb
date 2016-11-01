package com.bazaarvoice.emodb.sor.api;

import java.util.List;
import java.util.Map;

public interface CompactionControlSource {

    void updateStashTime(String id, long timestamp, List<String> placements, long expiredTimestamp, String dataCenter);

    void deleteStashTime(String id, String dataCenter);

    StashRunTimeInfo getStashTime(String id, String dataCenter);

    Map<String, StashRunTimeInfo> getAllStashTimes();

    Map<String, StashRunTimeInfo> getStashTimesForPlacement(String placement);
}