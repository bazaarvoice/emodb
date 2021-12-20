package com.bazaarvoice.emodb.cachemgr.invalidate;

import com.bazaarvoice.emodb.cachemgr.api.InvalidationEvent;
import com.bazaarvoice.emodb.cachemgr.api.InvalidationScope;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.ExecutorServiceManager;
import io.dropwizard.util.Duration;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class DefaultInvalidationProvider implements RemoteInvalidationProvider {

    private final EndPointProvider _localDataCenterEndPointProvider;
    private final EndPointProvider _foreignDataCenterEndPointProvider;
    private final RemoteInvalidationClient _invalidationClient;
    private final ExecutorService _executor;

    @Inject
    public DefaultInvalidationProvider(LifeCycleRegistry lifeCycle,
                                       @LocalDataCenter EndPointProvider localDataCenterEndPointProvider,
                                       @ForeignDataCenters EndPointProvider foreignDataCenterEndPointProvider,
                                       RemoteInvalidationClient invalidationClient) {
        _localDataCenterEndPointProvider = localDataCenterEndPointProvider;
        _foreignDataCenterEndPointProvider = foreignDataCenterEndPointProvider;
        _invalidationClient = invalidationClient;
        _executor = defaultInvalidationExecutor(lifeCycle);
    }

    private static ExecutorService defaultInvalidationExecutor(LifeCycleRegistry lifeCycle) {
        String nameFormat = "CacheMgr Invalidation-%d";
        ExecutorService executor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat(nameFormat).build());
        lifeCycle.manage(new ExecutorServiceManager(executor, Duration.seconds(5), nameFormat));
        return executor;
    }

    @Timed(name = "bv.emodb.cachemgr.DefaultInvalidationProvider.invalidateOtherServersInSameDataCenter", absolute = true)
    @Override
    public void invalidateOtherServersInSameDataCenter(InvalidationEvent event) {
        // Flush every individual server in our local data center.
        sendToAll(_localDataCenterEndPointProvider, InvalidationScope.LOCAL, event);
    }

    @Timed(name = "bv.emodb.cachemgr.DefaultInvalidationProvider.invalidateOtherDataCenters", absolute = true)
    @Override
    public void invalidateOtherDataCenters(InvalidationEvent event) {
        // Send a request to each foreign data center and ask one server in that data center to, in turn
        // flush all the individual servers in its data center.
        sendToAll(_foreignDataCenterEndPointProvider, InvalidationScope.DATA_CENTER, event);
    }

    private void sendToAll(EndPointProvider provider, final InvalidationScope scope, final InvalidationEvent event) {
        provider.withEndPoints(new Function<Collection<EndPoint>, Object>() {
            @Override
            public Object apply(Collection<EndPoint> endPoints) {
                sendToAll(endPoints, scope, event);
                return null;
            }
        });
    }

    private void sendToAll(Collection<EndPoint> endPoints, final InvalidationScope scope, final InvalidationEvent event) {
        // Call the function on each end point, each call in a separate thread
        Map<EndPoint, Future<?>> futures = Maps.newHashMap();
        for (final EndPoint endPoint : endPoints) {
            futures.put(endPoint, _executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    _invalidationClient.invalidateAll(endPoint.getAddress(), scope, event);
                    return null;
                }
            }));
        }

        // Check that every call succeeded
        for (Map.Entry<EndPoint, Future<?>> entry : futures.entrySet()) {
            EndPoint endPoint = entry.getKey();
            Future<?> future = entry.getValue();
            try {
                future.get();
            } catch (Exception e) {
                // Ignore hosts that go down while we're waiting for a response.
                if (endPoint.isValid()) {
                    Throwables.propagateIfPossible(e);
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
