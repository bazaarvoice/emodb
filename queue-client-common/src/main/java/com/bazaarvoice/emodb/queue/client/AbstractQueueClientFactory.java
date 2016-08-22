package com.bazaarvoice.emodb.queue.client;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.common.dropwizard.discovery.ServiceNames;
import com.bazaarvoice.emodb.queue.api.AuthQueueService;
import com.bazaarvoice.emodb.queue.api.QueueService;
import com.bazaarvoice.ostrich.MultiThreadedServiceFactory;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.partition.ConsistentHashPartitionFilter;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;

import java.net.URI;

/**
 * Abstract parent class for queue clients.  Subclasses are expected to create and configure an
 * {@link EmoClient} and then supply it via the constructor.
 */
abstract public class AbstractQueueClientFactory extends AbstractClientFactory<AuthQueueService> {
    private final String _clusterName;

    protected AbstractQueueClientFactory(String clusterName, EmoClient client) {
        super(client);
        _clusterName = clusterName;
    }

    @Override
    public String getServiceName() {
        return getServiceName(_clusterName);
    }

    protected static String getServiceName(String clusterName) {
        return ServiceNames.forNamespaceAndBaseServiceName(clusterName, QueueClient.BASE_SERVICE_NAME);
    }

    @Override
    public void configure(ServicePoolBuilder<AuthQueueService> servicePoolBuilder) {
        servicePoolBuilder.withPartitionFilter(new ConsistentHashPartitionFilter())
                .withPartitionContextAnnotationsFrom(QueueClient.class);
    }

    @Override
    protected AuthQueueService newClient(URI serviceUrl, boolean partitionSafe, EmoClient client) {
        return new QueueClient(serviceUrl, partitionSafe, client);
    }

    public MultiThreadedServiceFactory<QueueService> usingCredentials(final String apiKey) {
        final AbstractQueueClientFactory authServiceFactory = this;

        return new MultiThreadedServiceFactory<QueueService>() {
            @Override
            public String getServiceName() {
                return authServiceFactory.getServiceName();
            }

            @Override
            public void configure(ServicePoolBuilder<QueueService> servicePoolBuilder) {
                servicePoolBuilder.withPartitionFilter(new ConsistentHashPartitionFilter())
                        .withPartitionContextAnnotationsFrom(QueueServiceAuthenticatorProxy.class);
            }

            @Override
            public QueueService create(ServiceEndPoint endPoint) {
                AuthQueueService authQueueService = authServiceFactory.create(endPoint);
                return new QueueServiceAuthenticatorProxy(authQueueService, apiKey);
            }

            @Override
            public void destroy(ServiceEndPoint endPoint, QueueService service) {
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
