package com.bazaarvoice.emodb.cachemgr.invalidate;

import com.bazaarvoice.emodb.cachemgr.api.InvalidationEvent;
import com.bazaarvoice.emodb.cachemgr.api.InvalidationScope;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class DefaultInvalidationProviderTest {

    @Test
    public void testLocalInvalidation() {
        InvalidationEvent event = new InvalidationEvent(this, "tables", InvalidationScope.GLOBAL);

        LifeCycleRegistry lifeCycle = mock(LifeCycleRegistry.class);
        EndPointProvider local = getEndPointProvider(getEndPoint("local1", true), getEndPoint("local2", true));
        EndPointProvider foreign = mock(EndPointProvider.class);
        RemoteInvalidationClient client = mock(RemoteInvalidationClient.class);

        DefaultInvalidationProvider provider = new DefaultInvalidationProvider(lifeCycle, local, foreign, client);
        provider.invalidateOtherServersInSameDataCenter(event);

        verify(client).invalidateAll("http://local1:8081/tasks/invalidate", InvalidationScope.LOCAL, event);
        verify(client).invalidateAll("http://local2:8081/tasks/invalidate", InvalidationScope.LOCAL, event);
        verifyNoMoreInteractions(client);
        verifyZeroInteractions(foreign);
    }

    @Test
    public void testForeignInvalidation() {
        List<String> keys = Arrays.asList("1234", "5678");
        InvalidationEvent event = new InvalidationEvent(this, "tables", InvalidationScope.GLOBAL, keys);

        LifeCycleRegistry lifeCycle = mock(LifeCycleRegistry.class);
        EndPointProvider local = mock(EndPointProvider.class);
        EndPointProvider foreign = getEndPointProvider(getEndPoint("foreign1", true), getEndPoint("foreign2", true));
        RemoteInvalidationClient client = mock(RemoteInvalidationClient.class);

        DefaultInvalidationProvider provider = new DefaultInvalidationProvider(lifeCycle, local, foreign, client);
        provider.invalidateOtherDataCenters(event);

        verify(client).invalidateAll("http://foreign1:8081/tasks/invalidate", InvalidationScope.DATA_CENTER, event);
        verify(client).invalidateAll("http://foreign2:8081/tasks/invalidate", InvalidationScope.DATA_CENTER, event);
        verifyNoMoreInteractions(client);
        verifyZeroInteractions(local);
    }

    @Test
    public void testFailure() {
        InvalidationEvent event = new InvalidationEvent(this, "tables", InvalidationScope.GLOBAL);

        EndPointProvider foreign = getEndPointProvider(getEndPoint("foreign1", true), getEndPoint("foreign2", true));
        RemoteInvalidationClient client = mock(RemoteInvalidationClient.class);
        doThrow(IOException.class).when(client).invalidateAll("http://foreign2:8081/tasks/invalidate", InvalidationScope.DATA_CENTER, event);

        DefaultInvalidationProvider provider = new DefaultInvalidationProvider(
                mock(LifeCycleRegistry.class), mock(EndPointProvider.class), foreign, client);
        try {
            provider.invalidateOtherDataCenters(event);
            fail();
        } catch (RuntimeException e) {
            assertTrue(Throwables.getRootCause(e) instanceof IOException);
        }

        verify(client).invalidateAll("http://foreign1:8081/tasks/invalidate", InvalidationScope.DATA_CENTER, event);
        verify(client).invalidateAll("http://foreign2:8081/tasks/invalidate", InvalidationScope.DATA_CENTER, event);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testIgnorableFailure() {
        InvalidationEvent event = new InvalidationEvent(this, "tables", InvalidationScope.GLOBAL);

        EndPointProvider local = getEndPointProvider(getEndPoint("local1", false));
        RemoteInvalidationClient client = mock(RemoteInvalidationClient.class);
        doThrow(IOException.class).when(client).invalidateAll("http://local1:8081/tasks/invalidate", InvalidationScope.LOCAL, event);

        DefaultInvalidationProvider provider = new DefaultInvalidationProvider(
                mock(LifeCycleRegistry.class), local, mock(EndPointProvider.class), client);
        provider.invalidateOtherServersInSameDataCenter(event);

        verify(client).invalidateAll("http://local1:8081/tasks/invalidate", InvalidationScope.LOCAL, event);
        verifyNoMoreInteractions(client);
    }

    private EndPointProvider getEndPointProvider(final EndPoint... endPoints) {
        return new EndPointProvider() {
            @Override
            public void withEndPoints(Function<Collection<EndPoint>, ?> function) {
                function.apply(Arrays.asList(endPoints));
            }
        };
    }

    private EndPoint getEndPoint(final String host, final boolean valid) {
        return new EndPoint() {
            @Override
            public String getAddress() {
                return "http://" + host + ":8081/tasks/invalidate";
            }
            @Override
            public boolean isValid() {
                return valid;
            }
        };
    }
}
