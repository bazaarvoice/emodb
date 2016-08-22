package com.bazaarvoice.emodb.queue.client;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.common.dropwizard.discovery.ServiceNames;
import com.bazaarvoice.emodb.queue.api.AuthDedupQueueService;
import com.bazaarvoice.emodb.queue.api.DedupQueueService;
import com.bazaarvoice.ostrich.MultiThreadedServiceFactory;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.partition.ConsistentHashPartitionFilter;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;

import java.net.URI;

/**
 * Abstract parent class for dedup queue clients.  Subclasses are expected to create and configure an
 * {@link EmoClient} and then supply it via the constructor.
 */
abstract public class AbstractDedupQueueClientFactory extends AbstractClientFactory<AuthDedupQueueService> {
    private final String _clusterName;

    protected AbstractDedupQueueClientFactory(String clusterName, EmoClient client) {
        super(client);
        _clusterName = clusterName;
    }

    @Override
    public String getServiceName() {
        return getServiceName(_clusterName);
    }

    protected static String getServiceName(String clusterName) {
        return ServiceNames.forNamespaceAndBaseServiceName(clusterName, DedupQueueClient.BASE_SERVICE_NAME);
    }

    @Override
    public void configure(ServicePoolBuilder<AuthDedupQueueService> servicePoolBuilder) {
        servicePoolBuilder.withPartitionFilter(new ConsistentHashPartitionFilter())
                .withPartitionContextAnnotationsFrom(DedupQueueClient.class);
    }

    @Override
    protected AuthDedupQueueService newClient(URI serviceUrl, boolean partitionSafe, EmoClient client) {
        return new DedupQueueClient(serviceUrl, partitionSafe, client);
    }

    public MultiThreadedServiceFactory<DedupQueueService> usingCredentials(final String apiKey) {
        final AbstractDedupQueueClientFactory authServiceFactory = this;

        return new MultiThreadedServiceFactory<DedupQueueService>() {
            @Override
            public String getServiceName() {
                return authServiceFactory.getServiceName();
            }

            @Override
            public void configure(ServicePoolBuilder<DedupQueueService> servicePoolBuilder) {
                servicePoolBuilder.withPartitionFilter(new ConsistentHashPartitionFilter())
                        .withPartitionContextAnnotationsFrom(DedupQueueServiceAuthenticatorProxy.class);
            }

            @Override
            public DedupQueueService create(ServiceEndPoint endPoint) {
                AuthDedupQueueService authDedupQueueService = authServiceFactory.create(endPoint);
                return new DedupQueueServiceAuthenticatorProxy(authDedupQueueService, apiKey);
            }

            @Override
            public void destroy(ServiceEndPoint endPoint, DedupQueueService service) {
                // Nothing to do
            }

            @Override
            public boolean isHealthy(ServiceEndPoint endPoint) {
                return authServiceFactory.isHealthy(endPoint);
            }

            @Override
            public boolean isRetriableException(Exception exception) {
                return authServiceFactory.isRetriableException(exception);
            }
        };
    }
}
