package com.bazaarvoice.emodb.web.compactioncontrol;

import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.compactioncontrol.InMemoryCompactionControlSource;
import com.google.common.collect.ImmutableList;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.time.Clock;
import java.time.Duration;

import static org.mockito.Mockito.mock;

public class CompactionControlMonitorTest {

    @Test
    public void testExpiredTimestampsAreDeleted() {

        long timestamp1 = System.currentTimeMillis();
        long timestamp2 = System.currentTimeMillis() + Duration.ofHours(1).toMillis();
        long timestamp3 = System.currentTimeMillis() + Duration.ofHours(2).toMillis();

        long expiredTimestamp1 = timestamp1 + Duration.ofHours(10).toMillis();
        long expiredTimestamp2 = timestamp2 + Duration.ofHours(10).toMillis();
        long expiredTimestamp3 = timestamp3 + Duration.ofHours(10).toMillis();

        CompactionControlSource compactionControlSource = new InMemoryCompactionControlSource();
        compactionControlSource.updateStashTime("dc1-1", timestamp1, ImmutableList.of("placement-1"), expiredTimestamp1, "dc1");
        compactionControlSource.updateStashTime("dc1-2", timestamp2, ImmutableList.of("placement-1"), expiredTimestamp2, "dc1");
        compactionControlSource.updateStashTime("dc1-3", timestamp3, ImmutableList.of("placement-1"), expiredTimestamp3, "dc1");

        CompactionControlMonitor compactionControlMonitor = new CompactionControlMonitor(compactionControlSource, mock(Clock.class));

        compactionControlMonitor.deleteExpiredStashTimes(timestamp1 + Duration.ofMinutes(10).toMillis());
        Assert.assertEquals(compactionControlSource.getAllStashTimes().size(), 3);
        Assert.assertEquals(compactionControlSource.getAllStashTimes().containsKey("dc1-1"), true);

        compactionControlMonitor.deleteExpiredStashTimes(expiredTimestamp1 + Duration.ofMinutes(1).toMillis());
        Assert.assertEquals(compactionControlSource.getAllStashTimes().size(), 2);
        Assert.assertEquals(compactionControlSource.getAllStashTimes().containsKey("dc1-1"), false);

        compactionControlMonitor.deleteExpiredStashTimes(expiredTimestamp3 + Duration.ofMinutes(1).toMillis());
        Assert.assertEquals(compactionControlSource.getAllStashTimes().size(), 0);
        Assert.assertEquals(compactionControlSource.getAllStashTimes().containsKey("dc1-2"), false);
        Assert.assertEquals(compactionControlSource.getAllStashTimes().containsKey("dc1-3"), false);
    }

}
