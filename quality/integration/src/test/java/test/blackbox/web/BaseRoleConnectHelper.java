package test.blackbox.web;

import com.bazaarvoice.emodb.blob.api.AuthBlobStore;
import com.bazaarvoice.emodb.blob.client.BlobStoreClientFactory;
import com.bazaarvoice.emodb.blob.client.BlobStoreFixedHostDiscoverySource;
import com.bazaarvoice.emodb.client.uri.EmoUriBuilder;
import com.bazaarvoice.emodb.databus.api.AuthDatabus;
import com.bazaarvoice.emodb.databus.client.DatabusClientFactory;
import com.bazaarvoice.emodb.databus.client.DatabusFixedHostDiscoverySource;
import com.bazaarvoice.emodb.queue.api.AuthDedupQueueService;
import com.bazaarvoice.emodb.queue.api.AuthQueueService;
import com.bazaarvoice.emodb.queue.client.DedupQueueClientFactory;
import com.bazaarvoice.emodb.queue.client.QueueClientFactory;
import com.bazaarvoice.emodb.queue.client.QueueFixedHostDiscoverySource;
import com.bazaarvoice.emodb.sor.api.AuthDataStore;
import com.bazaarvoice.emodb.sor.client.DataStoreClientFactory;
import com.bazaarvoice.emodb.sor.client.DataStoreFixedHostDiscoverySource;
import com.bazaarvoice.emodb.web.EmoConfiguration;
import com.bazaarvoice.emodb.web.util.EmoServiceObjectMapperFactory;
import com.bazaarvoice.ostrich.discovery.zookeeper.ZooKeeperHostDiscovery;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.bazaarvoice.ostrich.retry.ExponentialBackoffRetry;
import com.bazaarvoice.ostrich.retry.RetryNTimes;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.io.Closer;
import com.google.common.net.HttpHeaders;

import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.jetty.ConnectorFactory;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.server.DefaultServerFactory;
import org.apache.curator.framework.CuratorFramework;
import org.testng.Assert;

import javax.validation.Validation;
import javax.validation.Validator;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;
import java.io.Closeable;
import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;


import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Blackbox tests run against actual EmoDB / C* processes started by mvn using emodb-sdk.
 * The 'ViaOstrich' methods use SOA so they should be connecting to the correct emodb node per role.
 * The 'ViaFixedHost' simulate ELB access, bypassing Ostrich SOA, allowing client to connect to the 'wrong' emodb node per role.
 */
public abstract class BaseRoleConnectHelper implements Closeable {

    private String _configFileResource;
    protected EmoConfiguration _config;
    protected Client _client;
    protected Closer closables = Closer.create();
    protected MetricRegistry _metricRegistry = new MetricRegistry();

    BaseRoleConnectHelper(String configFileResource) {
        try {
            _configFileResource = requireNonNull(configFileResource, "configFileResource");
            _config = requireNonNull(getConfigurationFromResource(), "EmoConfiguration");

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    public void close() {
        try {
            closables.close();
        } catch (Exception ignored) {
        }
    }

    protected EmoConfiguration getConfigurationFromResource() throws Exception {
        URL url = BaseRoleConnectHelper.class.getResource(_configFileResource);
        requireNonNull(url, _configFileResource);
        File file = new File(url.toURI());
        Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
        ObjectMapper mapper = EmoServiceObjectMapperFactory.build(new YAMLFactory());

        ConfigurationFactory<EmoConfiguration> configFactory = new ConfigurationFactory<>(EmoConfiguration.class, validator, mapper, "dw");
        return configFactory.build(file);
    }

    // BlobStore
    protected AuthBlobStore getBlobStoreViaOstrich() throws Exception {

        BlobStoreClientFactory clientFactory = BlobStoreClientFactory.forClusterAndHttpClient(
                _config.getCluster(), getClient());

        CuratorFramework curator = _config.getZooKeeperConfiguration().newCurator();
        curator.start();
        closables.register(curator);
        ZooKeeperHostDiscovery blobStoreDiscovery = new ZooKeeperHostDiscovery(curator, clientFactory.getServiceName(),
                _metricRegistry);
        closables.register(blobStoreDiscovery);
        return ServicePoolBuilder.create(AuthBlobStore.class)
                .withHostDiscovery(blobStoreDiscovery)
                .withServiceFactory(clientFactory)
                .withMetricRegistry(_metricRegistry)
                .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));
    }

    protected AuthBlobStore getBlobStoreViaFixedHost() throws JsonProcessingException {

        BlobStoreClientFactory clientFactory =
                BlobStoreClientFactory.forClusterAndHttpClient(_config.getCluster(), getClient());

        return ServicePoolBuilder.create(AuthBlobStore.class)
                .withHostDiscoverySource(new BlobStoreFixedHostDiscoverySource(getServiceBaseURI()))
                .withServiceFactory(clientFactory)
                .withMetricRegistry(_metricRegistry)
                .buildProxy(new RetryNTimes(600, 250, TimeUnit.MILLISECONDS));
    }

    // DataStore

    protected AuthDataStore getDataStoreViaOstrich() throws Exception {

        DataStoreClientFactory clientFactory =
                DataStoreClientFactory.forClusterAndHttpClient(
                        _config.getCluster(), getClient());

        CuratorFramework curator = _config.getZooKeeperConfiguration().newCurator();
        curator.start();
        closables.register(curator);
        ZooKeeperHostDiscovery datastoreHostDiscovery = new ZooKeeperHostDiscovery(curator, clientFactory.getServiceName(),
                _metricRegistry);
        closables.register(datastoreHostDiscovery);
        return ServicePoolBuilder.create(AuthDataStore.class)
                .withHostDiscovery(datastoreHostDiscovery)
                .withServiceFactory(clientFactory)
                .withMetricRegistry(_metricRegistry)
                .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));
    }

    protected AuthDataStore getDataStoreViaFixedHost() throws Exception {

        DataStoreClientFactory clientFactory =
                DataStoreClientFactory.forClusterAndHttpClient(
                        _config.getCluster(), getClient());

        return ServicePoolBuilder.create(AuthDataStore.class)
                .withHostDiscoverySource(new DataStoreFixedHostDiscoverySource(getServiceBaseURI()))
                .withServiceFactory(clientFactory)
                .withMetricRegistry(_metricRegistry)
                .buildProxy(new RetryNTimes(600, 250, TimeUnit.MILLISECONDS));
    }

    // Databus

    protected AuthDatabus getDatabusViaOstrich() throws Exception {

        DatabusClientFactory clientFactory =
                DatabusClientFactory.forClusterAndHttpClient(
                        _config.getCluster(), getClient());

        CuratorFramework curator = _config.getZooKeeperConfiguration().newCurator();
        curator.start();
        closables.register(curator);
        ZooKeeperHostDiscovery databusHostDiscovery = new ZooKeeperHostDiscovery(curator, clientFactory.getServiceName(),
                _metricRegistry);
        closables.register(databusHostDiscovery);
        return ServicePoolBuilder.create(AuthDatabus.class)
                .withHostDiscovery(databusHostDiscovery)
                .withServiceFactory(clientFactory)
                .withMetricRegistry(_metricRegistry)
                .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));
    }


    protected AuthDatabus getDatabusViaFixedHost() throws JsonProcessingException {

        DatabusClientFactory clientFactory =
                DatabusClientFactory.forClusterAndHttpClient(_config.getCluster(), getClient());

        return ServicePoolBuilder.create(AuthDatabus.class)
                .withHostDiscoverySource(new DatabusFixedHostDiscoverySource(getServiceBaseURI()))
                .withServiceFactory(clientFactory)
                .withMetricRegistry(_metricRegistry)
                .buildProxy(new RetryNTimes(600, 250, TimeUnit.MILLISECONDS));
    }

    // QueueService

    protected AuthQueueService getQueueServiceViaOstrich() throws Exception {

        QueueClientFactory clientFactory =
                QueueClientFactory.forClusterAndHttpClient(
                        _config.getCluster(), getClient());

        CuratorFramework curator = _config.getZooKeeperConfiguration().newCurator();
        curator.start();
        closables.register(curator);
        ZooKeeperHostDiscovery queueHostDiscovery = new ZooKeeperHostDiscovery(curator, clientFactory.getServiceName(),
                _metricRegistry);
        closables.register(queueHostDiscovery);
        return ServicePoolBuilder.create(AuthQueueService.class)
                .withHostDiscovery(queueHostDiscovery)
                .withServiceFactory(clientFactory)
                .withMetricRegistry(_metricRegistry)
                .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));
    }

    protected AuthQueueService getQueueServiceViaFixedHost() throws JsonProcessingException {

        QueueClientFactory clientFactory =
                QueueClientFactory.forClusterAndHttpClient(_config.getCluster(), getClient());

        return ServicePoolBuilder.create(AuthQueueService.class)
                .withHostDiscoverySource(new QueueFixedHostDiscoverySource(getServiceBaseURI()))
                .withServiceFactory(clientFactory)
                .withMetricRegistry(_metricRegistry)
                .buildProxy(new RetryNTimes(600, 250, TimeUnit.MILLISECONDS));
    }

    // DedupQueueService

    protected AuthDedupQueueService getDedupQueueServiceViaOstrich() throws Exception {

        DedupQueueClientFactory clientFactory =
                DedupQueueClientFactory.forClusterAndHttpClient(
                        _config.getCluster(), getClient());

        CuratorFramework curator = _config.getZooKeeperConfiguration().newCurator();
        curator.start();
        closables.register(curator);
        ZooKeeperHostDiscovery dedupqHostDiscovery = new ZooKeeperHostDiscovery(curator, clientFactory.getServiceName(),
                _metricRegistry);
        closables.register(dedupqHostDiscovery);
        return ServicePoolBuilder.create(AuthDedupQueueService.class)
                .withHostDiscovery(dedupqHostDiscovery)
                .withServiceFactory(clientFactory)
                .withMetricRegistry(_metricRegistry)
                .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));
    }

    protected Client getClient() {
        if (_client == null) {
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            _client = new JerseyClientBuilder(_metricRegistry).using(_config.getHttpClientConfiguration()).using(executorService, new ObjectMapper()).build("dw");
        }
        return _client;
    }

    // Jersey Client Access

    protected void httpPost(Map<String, Object> params, String... segments) throws Exception {
        // By default, post to admin ports.
        // Usually, you can use the SDK to post to service port
        httpPost(params, true, segments);

    }

    protected void httpPost(Map<String, Object> params, boolean isAdminPort, String... segments) throws Exception {

        UriBuilder builder = (isAdminPort) ? EmoUriBuilder.fromUri(URI.create(getAdminBaseURI()))
                : EmoUriBuilder.fromUri(URI.create(getServiceBaseURI()));
        builder.segment(segments);
        for (String lvalue : params.keySet()) {
            builder.queryParam(lvalue, params.get(lvalue));
        }

        URI uri = builder.build();

        //curl -XPOST http://localhost:8581/tasks/invalidate
        System.out.println(uri.toASCIIString());

        Client client = getClient();
        Response response = client.target(uri)
                .request()
                .header(HttpHeaders.AUTHORIZATION, null)
                .post(Entity.json(null));

        if (!response.getStatusInfo().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
            throw new WebApplicationException(response);
        }
    }

    protected List<?> httpGetServicePortAsList(Map<String, Object> params, String... segments) throws Exception {

        Client client = getClient();
        UriBuilder builder = EmoUriBuilder.fromUri(URI.create(getServiceBaseURI()));
        builder.segment(segments);
        for (String lvalue : params.keySet()) {
            builder.queryParam(lvalue, params.get(lvalue));
        }

        URI uri = builder.build();
        System.out.println(uri.toASCIIString());

        return client.target(uri)
                .request()
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .get(List.class);
    }

    protected String getAdminBaseURI() {
        int httpPort = 0;
        for (ConnectorFactory connector : ((DefaultServerFactory) _config.getServerFactory()).getAdminConnectors()) {
            if (connector.getClass().isAssignableFrom(HttpConnectorFactory.class)) {
                httpPort = ((HttpConnectorFactory) connector).getPort();
                break;
            }
        }

        return format("http://localhost:%d", httpPort);
    }

    protected String getServiceBaseURI() {
        int port = 0;
        for (ConnectorFactory connector : ((DefaultServerFactory) _config.getServerFactory()).getApplicationConnectors()) {
            if (connector.getClass().isAssignableFrom(HttpConnectorFactory.class)) {
                port = ((HttpConnectorFactory) connector).getPort();
                break;
            }
        }

        return format("http://localhost:%d", port);
    }

    protected int getServiceBasePort() {
        int port = 0;
        for (ConnectorFactory connector : ((DefaultServerFactory) _config.getServerFactory()).getApplicationConnectors()) {
            if (connector.getClass().isAssignableFrom(HttpConnectorFactory.class)) {
                port = ((HttpConnectorFactory) connector).getPort();
                break;
            }
        }

        return port;
    }
}
