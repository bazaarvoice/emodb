package com.bazaarvoice.emodb.common.cassandra.test;

import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.common.cassandra.health.CassandraHealthCheck;
import com.codahale.metrics.health.HealthCheck;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.common.base.Stopwatch;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.cql.CqlStatement;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Clock;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestCassandraHealthCheck {

    private static final String QUERY_STRING = "SELECT peer FROM system.peers LIMIT 1";
    private CqlStatement _astyanaxStatement;
    private Session _cqlSession;
    private Clock _clock;
    private CassandraHealthCheck _healthCheck;

    @BeforeMethod
    public void setUp() throws Exception {
        CassandraKeyspace keyspace = mock(CassandraKeyspace.class);

        _astyanaxStatement = mock(CqlStatement.class);
        when(_astyanaxStatement.withCql(QUERY_STRING)).thenReturn(_astyanaxStatement);
        when(_astyanaxStatement.withConsistencyLevel(any())).thenReturn(_astyanaxStatement);

        Keyspace astyanaxKeyspace = mock(Keyspace.class);
        when(astyanaxKeyspace.prepareCqlStatement()).thenReturn(_astyanaxStatement);

        when(keyspace.getAstyanaxKeyspace()).thenReturn(astyanaxKeyspace);

        _cqlSession = mock(Session.class);
        when(keyspace.getCqlSession()).thenReturn(_cqlSession);

        _clock = mock(Clock.class);

        _healthCheck = new CassandraHealthCheck(keyspace, QUERY_STRING, _clock);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testHealthCheckCaching() throws Exception {
        // Perform one positive health check to get it cached.
        OperationResult operationResult = createPositiveOperationResult("host1");
        ResultSet resultSet = createPositiveResultSet("host1");
        when(_astyanaxStatement.execute()).thenReturn(operationResult);
        when(_cqlSession.execute(QUERY_STRING)).thenReturn(resultSet);

        long now = 1478789200000L;
        when(_clock.millis()).thenReturn(now);

        HealthCheck.Result result = _healthCheck.execute();
        assertTrue(result.isHealthy());
        assertTrue(result.getMessage().contains("host1"));

        // Change the health checks to return a different host
        operationResult = createPositiveOperationResult("host2");
        resultSet = createPositiveResultSet("host2");
        when(_astyanaxStatement.execute()).thenReturn(operationResult);
        when(_cqlSession.execute(QUERY_STRING)).thenReturn(resultSet);

        // Move time forward 4.9 seconds to ensure the cached result is still returned
        when(_clock.millis()).thenReturn(now = now + 4900L);
        result = _healthCheck.execute();
        assertTrue(result.isHealthy());
        // Since the cached value should have been returned the old hosts should still be included in the message
        assertTrue(result.getMessage().contains("host1"));
        assertFalse(result.getMessage().contains("host2"));

        // Move time forward another 0.1 seconds for a total of 5.
        when(_clock.millis()).thenReturn(now + 100L);
        // Now the health check should perform the ping queries again
        result = _healthCheck.execute();
        assertTrue(result.isHealthy());
        assertTrue(result.getMessage().contains("host2"));
        assertFalse(result.getMessage().contains("host1"));
    }

    @SuppressWarnings({"ThrowableResultOfMethodCallIgnored", "unchecked"})
    @Test(timeOut = 15000L)
    public void testConcurrentHealthChecks() throws Exception {
        // Perform one positive health check to get it cached.
        OperationResult operationResult = createPositiveOperationResult("host1");
        ResultSet resultSet = createPositiveResultSet("host1");
        when(_astyanaxStatement.execute()).thenReturn(operationResult);
        when(_cqlSession.execute(QUERY_STRING)).thenReturn(resultSet);

        long now = 1478789200000L;
        when(_clock.millis()).thenReturn(now);

        HealthCheck.Result result = _healthCheck.execute();
        assertTrue(result.isHealthy());
        assertTrue(result.getMessage().contains("host1"));

        // Change the CQL health check to block for a controlled amount of time

        final CountDownLatch cqlBlocked = new CountDownLatch(1);
        final CountDownLatch raiseConnectionException = new CountDownLatch(1);

        when(_cqlSession.execute(QUERY_STRING)).thenAnswer(new Answer<ResultSet>() {
            @Override
            public ResultSet answer(InvocationOnMock invocationOnMock) throws Throwable {
                // Let the main thread know we are blocked
                cqlBlocked.countDown();
                // Wait for the main thread to give the signal to raise the connection exception
                raiseConnectionException.await();
                // Raise the exception
                throw new OperationException("simulated cassandra exception");
            }
        });

        // Move time forward 5 seconds to ensure the cached value isn't being returned.
        when(_clock.millis()).thenReturn(now = now + TimeUnit.SECONDS.toMillis(5));

        ExecutorService service = Executors.newFixedThreadPool(1);
        try {
            // In a new thread perform a health check
            Future<HealthCheck.Result> blockedThreadResult = service.submit(() -> _healthCheck.execute());
            // Wait for the thread's CQL call to be blocked
            assertTrue(cqlBlocked.await(5, TimeUnit.SECONDS), "Thread taking too long to make CQL call");

            // Make a call to the health check.  It should return immediately with the old healthy value
            result = _healthCheck.execute();
            assertTrue(result.isHealthy());
            assertTrue(result.getMessage().contains("host1"));

            // Move time forward 29 seconds and check again
            when(_clock.millis()).thenReturn(now = now + TimeUnit.SECONDS.toMillis(29));
            result = _healthCheck.execute();
            assertTrue(result.isHealthy());
            assertTrue(result.getMessage().contains("host1"));

            // Now move time forward one more second for a total of 30.   At this point this health check should return
            // unhealthy because the other health check is taking too long.
            when(_clock.millis()).thenReturn(now + TimeUnit.SECONDS.toMillis(1));
            Stopwatch stopWatch = Stopwatch.createStarted();
            result = _healthCheck.execute();
            stopWatch.stop();
            // Health check shouldn't have taken long
            assertTrue(stopWatch.elapsed(TimeUnit.SECONDS) < 1, "Heath check should not have been blocked");
            assertFalse(result.isHealthy());
            assertEquals(result.getMessage(), "Asynchronous health check update is taking too long");

            // Unblock the thread's health check and let it finish
            raiseConnectionException.countDown();

            result = blockedThreadResult.get(2, TimeUnit.SECONDS);
            assertFalse(result.isHealthy());
            assertTrue(result.getError() instanceof OperationException);

        } catch (Exception e) {
            // Always ensure the thread completes
            raiseConnectionException.countDown();
            service.shutdownNow();
        }
    }

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    @Test
    public void testNegativeHealthCheck() throws Exception {
        when(_astyanaxStatement.execute()).thenThrow(new OperationException("simulated cassandra exception"));
        when(_clock.millis()).thenReturn(1478789200000L);

        HealthCheck.Result result = _healthCheck.execute();
        assertFalse(result.isHealthy());
        assertTrue(result.getError() instanceof OperationException);
    }

    private OperationResult createPositiveOperationResult(String hostName) {
        OperationResult operationResult = mock(OperationResult.class);
        com.netflix.astyanax.connectionpool.Host host = mock(com.netflix.astyanax.connectionpool.Host.class);
        when(host.toString()).thenReturn(hostName);
        when(operationResult.getHost()).thenReturn(host);
        when(operationResult.getAttemptsCount()).thenReturn(1);
        return operationResult;
    }

    private ResultSet createPositiveResultSet(String hostName) {
        ExecutionInfo executionInfo = mock(ExecutionInfo.class);
        Host host = mock(Host.class);
        when(host.toString()).thenReturn(hostName);
        when(executionInfo.getQueriedHost()).thenReturn(host);
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getExecutionInfo()).thenReturn(executionInfo);
        return resultSet;
    }
}
