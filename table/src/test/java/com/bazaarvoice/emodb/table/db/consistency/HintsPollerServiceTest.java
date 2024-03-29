package com.bazaarvoice.emodb.table.db.consistency;

import com.bazaarvoice.emodb.common.zookeeper.store.ValueStore;
import com.bazaarvoice.emodb.common.zookeeper.store.ValueStoreListener;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.Session;
import com.google.common.collect.Sets;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class HintsPollerServiceTest {

    public final Session _session = mock(Session.class);

    @Test
    public void testPollForHintsWhenAHintIsFound()
            throws Exception {

        ClusterHintsPoller clusterHintsPoller = mock(ClusterHintsPoller.class);

        long currentTimestamp = System.currentTimeMillis();
        when(clusterHintsPoller.getOldestHintsInfo(_session)).thenReturn(new HintsPollerResult()
                .setHintsResult(InetAddress.getByName("127.0.0.1"), Optional.of(currentTimestamp)));

        ValueStore<Long> timestamp = new TestValueStore<>();

        HintsPollerService hintsPollerService = new HintsPollerService("emo-cluster", timestamp, _session, clusterHintsPoller, new MetricRegistry());
        hintsPollerService.pollForHints();

        long expectedTimestamp = currentTimestamp - (HintsPollerService.CASSANDRA_RPC_TIMEOUT.toMillis() * 2);
        assertEquals((long) hintsPollerService.getTimestamp().get(), expectedTimestamp);
    }

    @Test
    public void testPollForHintsForOldestHintTimestampWhenMultipleHintsAreFound()
            throws Exception {
        ClusterHintsPoller clusterHintsPoller = mock(ClusterHintsPoller.class);

        long currentTimestamp = System.currentTimeMillis();
        long laterTimestamp = currentTimestamp + 10000;
        when(clusterHintsPoller.getOldestHintsInfo(_session)).thenReturn(new HintsPollerResult()
                .setHintsResult(InetAddress.getByName("127.0.0.1"), Optional.of(laterTimestamp))
                .setHintsResult(InetAddress.getByName("127.0.0.2"), Optional.of(currentTimestamp)));

        ValueStore<Long> timestamp = new TestValueStore<>();
        HintsPollerService hintsPollerService = new HintsPollerService("emo-cluster", timestamp, _session, clusterHintsPoller, new MetricRegistry());
        hintsPollerService.pollForHints();

        long expectedTimestamp = currentTimestamp - (HintsPollerService.CASSANDRA_RPC_TIMEOUT.toMillis() * 2);
        assertEquals((long) hintsPollerService.getTimestamp().get(), expectedTimestamp);
    }

    @Test
    public void testPollForHintsWhenSomeNodeIsDown()
            throws Exception {
        ClusterHintsPoller clusterHintsPoller = mock(ClusterHintsPoller.class);

        when(clusterHintsPoller.getOldestHintsInfo(_session)).thenReturn(new HintsPollerResult()
                .setHostWithFailure(InetAddress.getByName("127.0.0.1")));

        ValueStore<Long> timestamp = new TestValueStore<>();

        HintsPollerService hintsPollerService = new HintsPollerService("emo-cluster", timestamp, _session, clusterHintsPoller, new MetricRegistry());

        hintsPollerService.pollForHints();

        assertNull(hintsPollerService.getTimestamp().get());
    }

    @Test
    public void testPollForHintsWhenNoHintsAreFound()
            throws Exception {
        ClusterHintsPoller clusterHintsPoller = mock(ClusterHintsPoller.class);

        when(clusterHintsPoller.getOldestHintsInfo(_session)).thenReturn(new HintsPollerResult()
                .setHintsResult(InetAddress.getByName("127.0.0.1"), Optional.empty())
                .setHintsResult(InetAddress.getByName("127.0.0.2"), Optional.empty()));

        ValueStore<Long> timestamp = new TestValueStore<>();
        // Since no hints were found, we should expect the hints poll time to get updated to some time later than this timestamp
        long baseTime = System.currentTimeMillis() - (HintsPollerService.CASSANDRA_RPC_TIMEOUT.toMillis() * 2);

        HintsPollerService hintsPollerService = new HintsPollerService("emo-cluster", timestamp, _session, clusterHintsPoller, new MetricRegistry());
        hintsPollerService.pollForHints();

        assertTrue(baseTime <= hintsPollerService.getTimestamp().get(), "Hints polled time was not updated correctly");
    }

    @Test
    public void testInfoLogsWhenRingIsUpdated()
            throws Exception {
        ClusterHintsPoller clusterHintsPoller = mock(ClusterHintsPoller.class);

        when(clusterHintsPoller.getOldestHintsInfo(_session)).thenReturn(new HintsPollerResult()
                .setHintsResult(InetAddress.getByName("127.0.0.1"), Optional.empty())
                .setHintsResult(InetAddress.getByName("127.0.0.3"), Optional.empty()));

        ValueStore<Long> timestamp = new TestValueStore<>();

        HintsPollerService hintsPollerService = new HintsPollerService("emo-cluster", timestamp, _session, clusterHintsPoller, new MetricRegistry());
        hintsPollerService._hosts = Sets.newHashSet(InetAddress.getByName("127.0.0.1"), InetAddress.getByName("127.0.0.2"));
        hintsPollerService.pollForHints();
    }

    private static class TestValueStore<T> implements ValueStore<T> {
        private volatile T _value = null;

        @Override
        public T get() {
            return _value;
        }

        @Override
        public void set(T value)
                throws Exception {
            _value = value;
        }

        @Override
        public void addListener(ValueStoreListener listener) {
            // nothing....
        }

        @Override
        public void removeListener(ValueStoreListener listener) {
            // nothing.....
        }
    }
}