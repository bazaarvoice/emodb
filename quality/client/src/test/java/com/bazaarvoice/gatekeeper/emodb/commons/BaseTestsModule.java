package com.bazaarvoice.gatekeeper.emodb.commons;

import com.bazaarvoice.curator.dropwizard.ZooKeeperConfiguration;
import com.bazaarvoice.emodb.blob.api.AuthBlobStore;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.blob.client.BlobStoreAuthenticator;
import com.bazaarvoice.emodb.blob.client.BlobStoreClientFactory;
import com.bazaarvoice.emodb.databus.api.AuthDatabus;
import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.databus.client.DatabusAuthenticator;
import com.bazaarvoice.emodb.databus.client.DatabusClientFactory;
import com.bazaarvoice.emodb.queue.api.DedupQueueService;
import com.bazaarvoice.emodb.queue.api.QueueService;
import com.bazaarvoice.emodb.queue.client.DedupQueueClientFactory;
import com.bazaarvoice.emodb.queue.client.QueueClientFactory;
import com.bazaarvoice.emodb.sor.api.AuthDataStore;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.client.DataStoreAuthenticator;
import com.bazaarvoice.emodb.sor.client.DataStoreClientFactory;
import com.bazaarvoice.emodb.uac.api.AuthUserAccessControl;
import com.bazaarvoice.emodb.uac.api.UserAccessControl;
import com.bazaarvoice.emodb.uac.client.UserAccessControlAuthenticator;
import com.bazaarvoice.emodb.uac.client.UserAccessControlClientFactory;
import com.bazaarvoice.gatekeeper.emodb.commons.annotations.ApiKeyTestDataStore;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.ServiceFactory;
import com.bazaarvoice.ostrich.discovery.zookeeper.ZooKeeperHostDiscovery;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.bazaarvoice.ostrich.retry.ExponentialBackoffRetry;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.client.apache4.ApacheHttpClient4;
import com.sun.jersey.client.apache4.ApacheHttpClient4Handler;
import com.sun.jersey.client.apache4.config.ApacheHttpClient4Config;
import com.sun.jersey.client.apache4.config.DefaultApacheHttpClient4Config;
import io.dropwizard.client.HttpClientBuilder;
import io.dropwizard.client.HttpClientConfiguration;
import io.dropwizard.util.Duration;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class BaseTestsModule extends AbstractModule {
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseTestsModule.class);

    private final Map<String, String> params;

    public BaseTestsModule(Map<String, String> params) {
        this.params = params;
    }

    @Override
    protected void configure() {
        Names.bindProperties(binder(), params);
    }

    @Provides
    @Singleton
    @Named("runID")
    private String getRunId() {
        return UUID.randomUUID().toString();
    }

    @Provides
    @Singleton
    @Named("curator")
    private CuratorFramework getCurator(@Named("zkConnection") String zkConnection, @Named("zkNamespace") String zkNamespace) {
        Preconditions.checkNotNull(zkConnection, "zooKeeperConnection not configured");
        ZooKeeperConfiguration zkConfiguration = new ZooKeeperConfiguration();
        zkConfiguration.setConnectString(zkConnection);
        zkConfiguration.setRetryPolicy(new BoundedExponentialBackoffRetry(100, 10000, 10));
        LOGGER.info("zkNamespace:" + zkNamespace);
        LOGGER.info("zkConnection:" + zkConnection);
        if (!zkNamespace.equals("")) {
            zkConfiguration.setNamespace(zkNamespace);
        }
        CuratorFramework curator = zkConfiguration.newCurator();
        curator.start();
        return curator;
    }

    @Provides
    @Singleton
    private HttpClientConfiguration createHttpClientConfiguration(@Named("clientHttpTimeout") String clientHttpTimeout,
                                                                  @Named("clientHttpKeepAlive") String clientHttpKeepAlive) {
        HttpClientConfiguration httpClientConfiguration = new HttpClientConfiguration();
        LOGGER.debug("Setting httpClientConfiguration timeout to {} seconds", clientHttpTimeout);
        httpClientConfiguration.setConnectionTimeout(Duration.seconds(10));
        httpClientConfiguration.setTimeout(Duration.seconds(Integer.parseInt(clientHttpTimeout)));
        LOGGER.debug("Setting httpClientConfiguration keepAlive to {} seconds", clientHttpKeepAlive);
        httpClientConfiguration.setKeepAlive(Duration.seconds(Integer.parseInt(clientHttpKeepAlive)));
        return httpClientConfiguration;
    }

    @Provides
    @Singleton
    private ApacheHttpClient4 createA4Client(HttpClientConfiguration httpClientConfiguration) {
        HttpClient httpClient = new HttpClientBuilder(new MetricRegistry()).using(httpClientConfiguration).build("Gatekeeper");
        ApacheHttpClient4Handler handler = new ApacheHttpClient4Handler(httpClient, null, true);
        ApacheHttpClient4Config config = new DefaultApacheHttpClient4Config();
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("drop", "admin"));
        config.getProperties().put(ApacheHttpClient4Config.PROPERTY_CREDENTIALS_PROVIDER, credentialsProvider);
        ApacheHttpClient4 a4Client = new ApacheHttpClient4(handler, config);
        a4Client.addFilter(new HTTPBasicAuthFilter("drop", "admin"));
        return a4Client;
    }

    @Provides
    @Singleton
    @Named("emodbHost")
    private String getEmodbHost(@Named("clusterName") String clusterName, HttpClientConfiguration httpClientConfiguration,
                                  @Named("curator") CuratorFramework curator) {
        DataStoreClientFactory factory = DataStoreClientFactory.forClusterAndHttpConfiguration(clusterName, httpClientConfiguration, new MetricRegistry());
        Iterator<ServiceEndPoint> serviceEndPoints = new ZooKeeperHostDiscovery(curator, factory.getServiceName(), new MetricRegistry()).getHosts().iterator();
        String emodbHost = "http://" + serviceEndPoints.next().getId();
        LOGGER.info("emodbHost: " + emodbHost);
        return emodbHost;
    }

    private static <T> T createService(Class<T> type, ServiceFactory<T> factory, CuratorFramework curator) {
        MetricRegistry metricRegistry = new MetricRegistry();
        return ServicePoolBuilder.create(type)
                .withHostDiscovery(new ZooKeeperHostDiscovery(curator, factory.getServiceName(), metricRegistry))
                .withServiceFactory(factory)
                .withMetricRegistry(metricRegistry)
                .buildProxy(new ExponentialBackoffRetry(10, 100, 10000, TimeUnit.MILLISECONDS));
    }
    
    /*
        <DataStore>
     */

    @Provides
    private AuthDataStore createAuthDataStore(@Named("curator") CuratorFramework curator,
                                                HttpClientConfiguration httpClientConfiguration, @Named("clusterName") String clusterName) {
        ServiceFactory<AuthDataStore> factory = DataStoreClientFactory
                .forClusterAndHttpConfiguration(clusterName, httpClientConfiguration, new MetricRegistry());
        return createService(AuthDataStore.class, factory, curator);
    }

    @Provides
    @Singleton
    private DataStoreAuthenticator createDataStoreAuthenticator(AuthDataStore authDataStore) {
        return DataStoreAuthenticator.proxied(authDataStore);
    }

    @Provides
    private DataStore createDataStore(@Named("apiKey") String apiKey, DataStoreAuthenticator dataStoreAuthenticator) {
        return dataStoreAuthenticator.usingCredentials(apiKey);
    }

    @Provides
    @ApiKeyTestDataStore
    private DataStore createApiDataStore(@Named("apiKey") String apiKey, @Named("clusterName") String clusterName,
                                           @Named("curator") CuratorFramework curator, ApacheHttpClient4 a4Client) {
        ServiceFactory<DataStore> factory = DataStoreClientFactory
                .forClusterAndHttpClient(clusterName, a4Client)
                .usingCredentials(apiKey);
        return createService(DataStore.class, factory, curator);
    }
    
    /*
        </DataStore>
     */

    @Provides
    private AuthDatabus createAuthDatabus(@Named("curator") CuratorFramework curator,
                                            HttpClientConfiguration httpClientConfiguration, @Named("clusterName") String clusterName) {
        ServiceFactory<AuthDatabus> factory = DatabusClientFactory
                .forClusterAndHttpConfiguration(clusterName, httpClientConfiguration, new MetricRegistry());
        return createService(AuthDatabus.class, factory, curator);
    }

    @Provides
    @Singleton
    private DatabusAuthenticator createDatabusAuthenticator(AuthDatabus authDatabus) {
        return DatabusAuthenticator.proxied(authDatabus);
    }

    @Provides
    @Singleton
    private Databus createDatabus(@Named("apiKey") String apiKey, DatabusAuthenticator databusAuthenticator) {
        return databusAuthenticator.usingCredentials(apiKey);
    }
    
    /*
        <UAC>
     */

    @Provides
    private AuthUserAccessControl creteAuthUserAccessControl(@Named("curator") CuratorFramework curator, @Named("clusterName") String clusterName,
                                                               HttpClientConfiguration httpClientConfiguration) {

        ServiceFactory<AuthUserAccessControl> factory = UserAccessControlClientFactory
                .forClusterAndHttpConfiguration(clusterName, httpClientConfiguration, new MetricRegistry());
        return createService(AuthUserAccessControl.class, factory, curator);
    }

    @Provides
    @Singleton
    private UserAccessControlAuthenticator createUserAccessControlAuthenticator(AuthUserAccessControl authUserAccessControl) {
        return UserAccessControlAuthenticator.proxied(authUserAccessControl);
    }

    @Provides
    @Singleton
    private UserAccessControl createUserAccessControl(UserAccessControlAuthenticator uacAuthenticator, @Named("apiKey") String apiKey) {
        return uacAuthenticator.usingCredentials(apiKey);
    }

    /*
        <Queue>
     */
    @Provides
    @Singleton
    private QueueService createQueueService(@Named("clusterName") String clusterName, @Named("curator") CuratorFramework curator,
                                              @Named("apiKey") String apiKey,
                                              HttpClientConfiguration httpClientConfiguration) {
        ServiceFactory<QueueService> factory = QueueClientFactory
                .forClusterAndHttpConfiguration(clusterName, httpClientConfiguration, new MetricRegistry())
                .usingCredentials(apiKey);
        return createService(QueueService.class, factory, curator);
    }

    /*
        <DedupeQueue>
     */
    @Provides
    @Singleton
    private DedupQueueService createDedupQueueService(@Named("clusterName") String clusterName, @Named("curator") CuratorFramework curator,
                                                        @Named("apiKey") String apiKey, HttpClientConfiguration httpClientConfiguration) {
        ServiceFactory<DedupQueueService> factory = DedupQueueClientFactory
                .forClusterAndHttpConfiguration(clusterName, httpClientConfiguration, new MetricRegistry())
                .usingCredentials(apiKey);
        return createService(DedupQueueService.class, factory, curator);
    }
    
    /*
        </BlobStore>
     */

    @Provides
    private AuthBlobStore createAuthBlobStore(@Named("curator") CuratorFramework curator,
                                                HttpClientConfiguration httpClientConfiguration, @Named("clusterName") String clusterName) {
        ServiceFactory<AuthBlobStore> factory = BlobStoreClientFactory
                .forClusterAndHttpConfiguration(clusterName, httpClientConfiguration, new MetricRegistry());
        return createService(AuthBlobStore.class, factory, curator);
    }

    @Provides
    @Singleton
    private BlobStoreAuthenticator createBlobStoreAuthenticator(AuthBlobStore authBlobStore) {
        return BlobStoreAuthenticator.proxied(authBlobStore);
    }

    @Provides
    @Singleton
    private BlobStore createBlobStore(@Named("apiKey") String apiKey, BlobStoreAuthenticator blobStoreAuthenticator) {
        return blobStoreAuthenticator.usingCredentials(apiKey);
    }
}
