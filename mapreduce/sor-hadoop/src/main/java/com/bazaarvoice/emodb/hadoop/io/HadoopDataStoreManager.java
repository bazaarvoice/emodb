package com.bazaarvoice.emodb.hadoop.io;

import com.bazaarvoice.emodb.common.dropwizard.discovery.Payload;
import com.bazaarvoice.emodb.sor.api.AuthDataStore;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.client.DataStoreAuthenticator;
import com.bazaarvoice.emodb.sor.client.DataStoreClientFactory;
import com.bazaarvoice.ostrich.HostDiscovery;
import com.bazaarvoice.ostrich.MultiThreadedServiceFactory;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.ServiceEndPointBuilder;
import com.bazaarvoice.ostrich.pool.ServiceCachingPolicyBuilder;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.bazaarvoice.ostrich.pool.ServicePoolProxies;
import com.bazaarvoice.ostrich.retry.ExponentialBackoffRetry;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.google.common.reflect.AbstractInvocationHandler;
import io.dropwizard.client.HttpClientConfiguration;
import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.client.JerseyClientConfiguration;
import io.dropwizard.util.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.ws.rs.client.Client;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Singleton class which provides CloseableDataStores to the callers.  It reuses the same instance as much as possible
 * until it is no longer referenced.  This is to reduce the number of ZooKeeper connections when operations are
 * performed from the same DataSource concurrently by the same process.
 */
public class HadoopDataStoreManager {

    private final Logger _log = LoggerFactory.getLogger(HadoopDataStoreManager.class);

    private static final HadoopDataStoreManager _instance = new HadoopDataStoreManager();

    // Caches DataStores by location and configuration
    private final ConcurrentMap<String, DataStoreMonitor> _dataStoreByLocation = Maps.newConcurrentMap();

    public static HadoopDataStoreManager getInstance() {
        return _instance;
    }

    private HadoopDataStoreManager() {
        // empty
    }

    /**
     * Returns a DataStore for a given location.  If a cached instance already exists its reference count is incremented
     * and returned, otherwise a new instance is created and cached.
     */
    public CloseableDataStore getDataStore(URI location, String apiKey, MetricRegistry metricRegistry)
            throws IOException {
        String id = LocationUtil.getDataStoreIdentifier(location, apiKey);
        CloseableDataStore dataStore = null;
        while (dataStore == null) {
            // Get the cached DataStore if it exists
            DataStoreMonitor dataStoreMonitor = _dataStoreByLocation.get(id);
            if (dataStoreMonitor == null || (dataStore = dataStoreMonitor.getDataStore()) == null) {
                // Either the value wasn't cached or the cached value was closed before the reference count could
                // be incremented.  Create a new DataStore and cache it.
                CloseableDataStore unmonitoredDataStore;
                switch (LocationUtil.getLocationType(location)) {
                    case EMO_HOST_DISCOVERY:
                        unmonitoredDataStore = createDataStoreWithHostDiscovery(location, apiKey, metricRegistry);
                        break;
                    case EMO_URL:
                        unmonitoredDataStore = createDataStoreWithUrl(location, apiKey, metricRegistry);
                        break;
                    default:
                        throw new IllegalArgumentException("Location does not use a data store: " + location);
                }

                dataStoreMonitor = new DataStoreMonitor(id, unmonitoredDataStore);

                if (_dataStoreByLocation.putIfAbsent(id, dataStoreMonitor) != null) {
                    // Race condition; close the created DataStore and try again
                    dataStoreMonitor.closeNow();
                } else {
                    // New data store was cached; return the value
                    dataStore = dataStoreMonitor.getDataStore();
                }
            }
        }

        return dataStore;
    }

    /**
     * Creates a DataStore using host discovery (ZooKeeper and Ostrich).
     */
    private CloseableDataStore createDataStoreWithHostDiscovery(final URI location, String apiKey, MetricRegistry metricRegistry) {
        // Get the service factory for this cluster.
        String cluster = LocationUtil.getClusterForLocation(location);
        MultiThreadedServiceFactory<AuthDataStore> dataStoreFactory = createDataStoreServiceFactory(cluster, metricRegistry);

        // If the host discovery uses ZooKeeper -- that is, it doesn't used fixed hosts -- create a Curator for it.
        final Optional<CuratorFramework> curator = LocationUtil.getCuratorForLocation(location);
        HostDiscovery hostDiscovery = LocationUtil.getHostDiscoveryForLocation(location, curator, dataStoreFactory.getServiceName(), metricRegistry);

        final AuthDataStore authDataStore = ServicePoolBuilder.create(AuthDataStore.class)
                .withHostDiscovery(hostDiscovery)
                .withServiceFactory(dataStoreFactory)
                .withCachingPolicy(ServiceCachingPolicyBuilder.getMultiThreadedClientPolicy())
                .withMetricRegistry(metricRegistry)
                .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));

        // Set the CloseableDataStore's close() method to close the service pool proxy and, if it was required, the curator.
        Runnable onClose = new Runnable() {
            @Override
            public void run() {
                _log.info("Closing service pool and ZooKeeper connection for {}", location);
                ServicePoolProxies.close(authDataStore);
                if (curator.isPresent()) {
                    try {
                        Closeables.close(curator.get(), true);
                    } catch (IOException e) {
                        // Won't happen, already caught
                    }
                }
            }
        };

        DataStore dataStore = DataStoreAuthenticator.proxied(authDataStore).usingCredentials(apiKey);
        return asCloseableDataStore(dataStore, Optional.of(onClose));
    }

    /**
     * Creates a DataStore using a URL (e.g: "https://emodb-ci.dev.us-east-1.nexus.bazaarvoice.com").
     */
    private CloseableDataStore createDataStoreWithUrl(URI location, String apiKey, MetricRegistry metricRegistry) {
        // Reuse the same base classes as when using host discovery, ignoring unused fields as needed.
        String ignore = "ignore";
        MultiThreadedServiceFactory<AuthDataStore> secureDataStoreFactory = createDataStoreServiceFactory(ignore, metricRegistry);

        URI baseUri = LocationUtil.getBaseUriForLocation(location);

        Payload payload = new Payload(baseUri, URI.create("http://adminUrl.not.used.but.required"));
        ServiceEndPoint endPoint = new ServiceEndPointBuilder()
                .withId(ignore)
                .withServiceName(ignore)
                .withPayload(payload.toString())
                .build();

        AuthDataStore authDataStore = secureDataStoreFactory.create(endPoint);
        DataStore dataStore = DataStoreAuthenticator.proxied(authDataStore).usingCredentials(apiKey);

        // No operations required when closing the DataStore, so use absent.
        return asCloseableDataStore(dataStore, Optional.<Runnable>absent());
    }

    /**
     * Creates a ServiceFactory for a cluster with reasonable configurations.
     */
    private MultiThreadedServiceFactory<AuthDataStore> createDataStoreServiceFactory(String cluster, MetricRegistry metricRegistry) {
        JerseyClientConfiguration clientConfig = new JerseyClientConfiguration();
        clientConfig.setKeepAlive(Duration.seconds(1));
        clientConfig.setConnectionTimeout(Duration.seconds(10));
        clientConfig.setTimeout(Duration.minutes(5));

        ExecutorService executorService = Executors.newFixedThreadPool(50);
        Client client = new JerseyClientBuilder(metricRegistry).using(clientConfig).using(executorService, new ObjectMapper()).build("dw");


        return DataStoreClientFactory.forClusterAndHttpClient(cluster, client);
    }

    /**
     * Creates a proxy which delegates all DataStore operations to the provided instance and delegates the Closeable's
     * close() method to the provided Runnable if necessary.
     */
    private CloseableDataStore asCloseableDataStore(final DataStore dataStore, final Optional<Runnable> onClose) {
        return (CloseableDataStore) Proxy.newProxyInstance(
                DataStore.class.getClassLoader(), new Class[] { CloseableDataStore.class },
                new AbstractInvocationHandler() {
                    @Override
                    protected Object handleInvocation(Object proxy, Method method, Object[] args)
                            throws Throwable {
                        if ("close".equals(method.getName())) {
                            if (onClose.isPresent()) {
                                onClose.get().run();
                            }
                            return null;
                        } else {
                            return method.invoke(dataStore, args);
                        }
                    }
                });
    }

    /**
     * DataStore monitor which performs reference counting.  Once fully dereferenced it closes the proxied DataStore.
     */
    private class DataStoreMonitor {
        private final String _id;
        private final CloseableDataStore _dataStore;
        private volatile int _referenceCount = 0;
        private volatile boolean _closed = false;

        public DataStoreMonitor(final String id, final CloseableDataStore dataStore) {
            _id = id;
            _dataStore = dataStore;
        }

        /**
         * Return the data store being monitored.
         */
        public CloseableDataStore getDataStore() {
            if (!incRef()) {
                return null;
            }

            Runnable onClose = new Runnable() {
                // Need to make sure closing this DataStore only updates the reference count on the first call to close()
                private AtomicBoolean _closed = new AtomicBoolean(false);

                @Override
                public void run() {
                    if (_closed.compareAndSet(false, true)) {
                        decRef();
                    }
                }
            };

            return asCloseableDataStore(_dataStore, Optional.of(onClose));
        }

        /**
         * Increment the number of references to the DataStore by one.
         * @return true if successful, false if the DataStore was already completely dereferenced and therefore closed
         */
        private synchronized boolean incRef() {
            if (_closed) {
                return false;
            }
            _referenceCount += 1;
            _log.info("Data store reference count updated: [id = {}, reference count = {}]", _id, _referenceCount);
            return true;
        }

        /**
         * Decrement the number of references to the DataStore by one.  If the DataStore is completely dereferenced
         * it will be closed.
         */
        private synchronized void decRef() {
            _referenceCount -= 1;
            _log.info("Data store reference count updated: [id = {}, reference count = {}]", _id, _referenceCount);

            if (_referenceCount == 0) {
                closeDataStore();
            }
        }

        private void closeDataStore() {
            // The DataStore is completely dereferenced
            _log.info("Removing data store: [id = {}]", _id);
            _dataStoreByLocation.remove(_id, this);
            try {
                Closeables.close(_dataStore, true);
            } catch (IOException e) {
                // Won't happen, already caught
            }
            _closed = true;
        }

        /**
         * Force the DataStore to close immediately
         */
        public synchronized void closeNow() {
            if (!_closed) {
                _referenceCount = 0;
                closeDataStore();
            }
        }
    }
}
