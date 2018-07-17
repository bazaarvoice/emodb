package com.bazaarvoice.emodb.sor.consistency;

import com.bazaarvoice.emodb.table.db.ClusterInfo;
import com.bazaarvoice.emodb.table.db.astyanax.FullConsistencyTimeProvider;
import com.bazaarvoice.emodb.table.db.consistency.CompositeConsistencyTimeProvider;
import com.bazaarvoice.emodb.table.db.consistency.HintsConsistencyTimeProvider;
import com.bazaarvoice.emodb.table.db.consistency.MinLagConsistencyTimeProvider;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.time.Duration;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class FullConsistencyTest {

    @Test
    public void testCompositeConsistencyTimeProvider() {

        // Mock FullConsistencyTimeProviders
        FullConsistencyTimeProvider hintsConsistencyTimeProvider = mock(HintsConsistencyTimeProvider.class);
        FullConsistencyTimeProvider minLagConsistencyTimeProvider = mock(MinLagConsistencyTimeProvider.class);

        // Testing hardcoded range so we always start compaction with at least a minute lag
        // and never leave rows uncompacted for more than 10 days

        Duration hardcodedMax = Duration.ofDays(10);
        Duration hardcodedMin = Duration.ofMinutes(1);
        // Manually set the lag to 5 seconds
        long minLag = 5000L;
        long currentTimestamp = System.currentTimeMillis();
        when(hintsConsistencyTimeProvider.getMaxTimeStamp(anyString())).thenReturn(currentTimestamp);
        when(minLagConsistencyTimeProvider.getMaxTimeStamp(anyString())).thenReturn(currentTimestamp - minLag);

        // Make sure that we get back the hard-coded range from CompositeConsistencyTimeProvider
        CompositeConsistencyTimeProvider compositeConsistencyTimeProvider = new CompositeConsistencyTimeProvider(
                ImmutableList.of(new ClusterInfo("c1", "c1_metric_name")),
                ImmutableList.of(hintsConsistencyTimeProvider, minLagConsistencyTimeProvider),
                new MetricRegistry());

        // Since we manually set a lag of 5 seconds, verify that we still get back at least 1 minute lag
        assertTrue(compositeConsistencyTimeProvider.getMaxTimeStamp(anyString()) <=
                System.currentTimeMillis() - hardcodedMin.toMillis(), "Minimum compaction lag is less than 1 minute");

        // Make the fullconsistency time stamp to return 11 days
        when(hintsConsistencyTimeProvider.getMaxTimeStamp(anyString())).thenReturn(currentTimestamp
                - Duration.ofDays(11).toMillis());

        // Since we manually set a consistency timestamp of 11 days, verify that we still get back at most 10 days lag
        assertTrue(System.currentTimeMillis() - hardcodedMax.toMillis() <=
                compositeConsistencyTimeProvider.getMaxTimeStamp(anyString()), "Maximum compaction lag is more than 10 days");

        // Test values within the hardcoded range
        // The minimum timestamp between the full consistency and lag is used for compaction

        minLag = Duration.ofMinutes(5).toMillis();
        long fct = currentTimestamp - Duration.ofMinutes(6).toMillis();
        when(minLagConsistencyTimeProvider.getMaxTimeStamp(anyString())).thenReturn(currentTimestamp - minLag);
        when(hintsConsistencyTimeProvider.getMaxTimeStamp(anyString())).thenReturn(fct);
        assertEquals(fct,
                compositeConsistencyTimeProvider.getMaxTimeStamp(anyString()), "Incorrect compaction timestamp is returned");

        fct = currentTimestamp - Duration.ofMinutes(3).toMillis();
        when(hintsConsistencyTimeProvider.getMaxTimeStamp(anyString())).thenReturn(fct);
        assertTrue(compositeConsistencyTimeProvider.getMaxTimeStamp(anyString()) < fct, "Minimum lag is violated");
        assertEquals(currentTimestamp - minLag, compositeConsistencyTimeProvider.getMaxTimeStamp(anyString()));
    }

}
