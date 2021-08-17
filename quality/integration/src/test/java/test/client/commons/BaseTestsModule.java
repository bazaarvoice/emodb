package test.client.commons;

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
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.ServiceFactory;
import com.bazaarvoice.ostrich.discovery.zookeeper.ZooKeeperHostDiscovery;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.bazaarvoice.ostrich.retry.ExponentialBackoffRetry;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.client.JerseyClientConfiguration;
import io.dropwizard.util.Duration;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import test.client.commons.annotations.ApiKeyTestDataStore;

import javax.ws.rs.client.Client;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executors;
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
        Objects.requireNonNull(zkConnection, "zooKeeperConnection not configured");
        LOGGER.info("zkNamespace:" + zkNamespace);
        LOGGER.info("zkConnection:" + zkConnection);
        ZooKeeperConfiguration zkConfiguration = new ZooKeeperConfiguration();
        zkConfiguration.setConnectString(zkConnection);
        zkConfiguration.setRetryPolicy(new BoundedExponentialBackoffRetry(100, 10000, 10));
        if (!zkNamespace.equals("")) {
            zkConfiguration.setNamespace(zkNamespace);
        }
        CuratorFramework curator = zkConfiguration.newCurator();
        curator.start();
        return curator;
    }

    @Provides
    @Singleton
    private JerseyClientConfiguration createClientConfiguration(@Named("clientHttpTimeout") String clientHttpTimeout,
                                                                  @Named("clientHttpKeepAlive") String clientHttpKeepAlive) {
        JerseyClientConfiguration httpClientConfiguration = new JerseyClientConfiguration();
        LOGGER.debug("Setting httpClientConfiguration timeout to {} seconds", clientHttpTimeout);
        httpClientConfiguration.setConnectionTimeout(Duration.seconds(10));
        httpClientConfiguration.setTimeout(Duration.seconds(Integer.valueOf(clientHttpTimeout)));
        LOGGER.debug("Setting httpClientConfiguration keepAlive to {} seconds", clientHttpKeepAlive);
        httpClientConfiguration.setKeepAlive(Duration.seconds(Integer.valueOf(clientHttpKeepAlive)));
        httpClientConfiguration.setGzipEnabledForRequests(false);

        return httpClientConfiguration;
    }

    @Provides
    @Singleton
    private Client createClient(JerseyClientConfiguration httpClientConfiguration, CredentialsProvider credentialsProvider) {
        Client client= new JerseyClientBuilder(new MetricRegistry())
                    .using(httpClientConfiguration)
                    .using(Executors.newSingleThreadExecutor())
                    .using(new ObjectMapper())
                .using(credentialsProvider)
                .build("Gatekeeper");
        client.register(JacksonJsonProvider.class);
        return client;
    }

    @Provides
    @Singleton
    private CredentialsProvider getCredentialsProvider() {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("drop", "admin"));
        return credentialsProvider;
    }

    @Provides
    @Singleton
    @Named("emodbHost")
    private String getEmodbHost(@Named("clusterName") String clusterName, Client client,
                                  @Named("curator") CuratorFramework curator) {
        Objects.requireNonNull(clusterName);
        Objects.requireNonNull(client);
        Objects.requireNonNull(curator);

        DataStoreClientFactory factory = DataStoreClientFactory.forClusterAndHttpClient(clusterName, client);
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
                                                Client client, @Named("clusterName") String clusterName) {
        ServiceFactory<AuthDataStore> factory = DataStoreClientFactory.forClusterAndHttpClient(clusterName, client);
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
                                           @Named("curator") CuratorFramework curator, Client client) {
        ServiceFactory<DataStore> factory = DataStoreClientFactory
                .forClusterAndHttpClient(clusterName, client)
                .usingCredentials(apiKey);
        return createService(DataStore.class, factory, curator);
    }

    /*
        </DataStore>
     */

    @Provides
    private AuthDatabus createAuthDatabus(@Named("curator") CuratorFramework curator, Client client, @Named("clusterName") String clusterName) {
        ServiceFactory<AuthDatabus> factory = DatabusClientFactory.forClusterAndHttpClient(clusterName, client);
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
                                                               Client client) {
        ServiceFactory<AuthUserAccessControl> factory = UserAccessControlClientFactory.forClusterAndHttpClient(clusterName, client);
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
                                              @Named("apiKey") String apiKey, Client client) {
        ServiceFactory<QueueService> factory = QueueClientFactory
                .forClusterAndHttpClient(clusterName, client)
                .usingCredentials(apiKey);
        return createService(QueueService.class, factory, curator);
    }

    /*
        <DedupeQueue>
     */
    @Provides
    @Singleton
    private DedupQueueService createDedupQueueService(@Named("clusterName") String clusterName, @Named("curator") CuratorFramework curator,
                                                        @Named("apiKey") String apiKey, Client client) {
        ServiceFactory<DedupQueueService> factory = DedupQueueClientFactory
                .forClusterAndHttpClient(clusterName, client)
                .usingCredentials(apiKey);
        return createService(DedupQueueService.class, factory, curator);
    }

    /*
        </BlobStore>
     */

    @Provides
    protected AuthBlobStore createAuthBlobStore(@Named("curator") CuratorFramework curator, Client client, @Named("clusterName") String clusterName) {
            ServiceFactory<AuthBlobStore> factory = BlobStoreClientFactory.forClusterAndHttpClient(clusterName, client);
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
