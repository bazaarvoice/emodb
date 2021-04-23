package com.bazaarvoice.emodb.web.partition;

import com.bazaarvoice.emodb.common.dropwizard.healthcheck.HealthCheckRegistry;
import com.bazaarvoice.ostrich.MultiThreadedServiceFactory;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import com.google.common.net.HostAndPort;
import org.apache.http.conn.ConnectTimeoutException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.ws.rs.ProcessingException;
import java.nio.BufferOverflowException;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@SuppressWarnings("unchecked")
public class PartitionAwareServiceFactoryTest {

    // Just an interface with a single method with a declared non-runtime exception
    public interface TestInterface {
        void doIt() throws JsonParseException;
    }

    private PartitionAwareServiceFactory<TestInterface> _serviceFactory;
    private MultiThreadedServiceFactory<TestInterface> _delegateServiceFactory;
    private TestInterface _local;
    private TestInterface _delegate;
    private MetricRegistry _metricRegistry;

    private ServiceEndPoint _localEndPoint = mock(ServiceEndPoint.class);
    private ServiceEndPoint _remoteEndPoint = mock(ServiceEndPoint.class);

    @BeforeClass
    public void setupEndPoints() {
        when(_localEndPoint.getId()).thenReturn("localhost:8080");
        when(_remoteEndPoint.getId()).thenReturn("remotehost:8080");
    }

    @BeforeMethod
    public void setUp() {
        _local = mock(TestInterface.class);
        _delegate = mock(TestInterface.class);

        _delegateServiceFactory = mock(MultiThreadedServiceFactory.class);
        when(_delegateServiceFactory.create(_remoteEndPoint)).thenReturn(_delegate);

        _metricRegistry = new MetricRegistry();

        _serviceFactory = new PartitionAwareServiceFactory<>(TestInterface.class, _delegateServiceFactory, _local,
                HostAndPort.fromParts("localhost", 8080), mock(HealthCheckRegistry.class), _metricRegistry);
    }

    @AfterMethod
    public void tearDown() {
        verifyNoMoreInteractions(_delegateServiceFactory, _local, _delegate);
    }

    @Test
    public void testLocalService() {
        TestInterface service = _serviceFactory.create(_localEndPoint);
        assertSame(service, _local);
    }

    @Test
    public void testDelegateService() throws Exception {
        TestInterface service = _serviceFactory.create(_remoteEndPoint);
        assertNotSame(service, _local);
        service.doIt();
        _serviceFactory.destroy(_remoteEndPoint, service);

        verify(_delegateServiceFactory).create(_remoteEndPoint);
        verify(_delegate).doIt();
        verify(_delegateServiceFactory).destroy(_remoteEndPoint, _delegate);
    }

    @Test
    public void testDelegateDeclaredExceptionPropagation() throws Exception {
        doThrow(new JsonParseException("Simulated declared exception", JsonLocation.NA)).when(_delegate).doIt();
        TestInterface service = _serviceFactory.create(_remoteEndPoint);

        try {
            service.doIt();
            fail("JsonParseException not thrown");
        } catch (JsonParseException e) {
            assertEquals(e.getOriginalMessage(), "Simulated declared exception");
        }

        assertEquals(_metricRegistry.getMeters().get("bv.emodb.web.partition-forwarding.TestInterface.errors").getCount(), 0);
        
        verify(_delegateServiceFactory).create(_remoteEndPoint);
        verify(_delegate).doIt();
    }

    @Test
    public void testDelegateUndeclaredExceptionPropagation() throws Exception {
        // Just need some undeclared runtime exception, so BufferOverflowException it is
        doThrow(new BufferOverflowException()).when(_delegate).doIt();
        TestInterface service = _serviceFactory.create(_remoteEndPoint);

        try {
            service.doIt();
        } catch (BufferOverflowException e) {
            // ok
        }

        assertEquals(_metricRegistry.getMeters().get("bv.emodb.web.partition-forwarding.TestInterface.errors").getCount(), 0);

        verify(_delegateServiceFactory).create(_remoteEndPoint);
        verify(_delegate).doIt();
    }

    @Test
    public void testDelegateConnectionTimeoutException() throws Exception {
        doThrow(new ProcessingException(new ConnectTimeoutException())).when(_delegate).doIt();
        TestInterface service = _serviceFactory.create(_remoteEndPoint);

        try {
            service.doIt();
        } catch (PartitionForwardingException e) {
            assertTrue(e.getCause() instanceof ConnectTimeoutException);
        }

        assertEquals(_metricRegistry.getMeters().get("bv.emodb.web.partition-forwarding.TestInterface.errors").getCount(), 1);

        verify(_delegateServiceFactory).create(_remoteEndPoint);
        verify(_delegate).doIt();

    }
}
