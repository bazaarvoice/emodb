package com.bazaarvoice.emodb.sor.client;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.common.dropwizard.discovery.Payload;
import com.bazaarvoice.emodb.sor.api.AuthDataStore;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.ostrich.MultiThreadedServiceFactory;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;

/**
 * Abstract parent class for data store clients.  Subclasses are expected to create and configure an
 * {@link EmoClient} and then supply it via the constructor.
 */
abstract public class AbstractDataStoreClientFactory extends AbstractDataStoreClientFactoryBase<AuthDataStore> {

    protected AbstractDataStoreClientFactory(String clusterName, EmoClient client) {
        super(clusterName, client);
    }

    @Override
    public AuthDataStore create(ServiceEndPoint endPoint) {
        Payload payload = Payload.valueOf(endPoint.getPayload());
        return new DataStoreClient(payload.getServiceUrl(), _client);
    }

    public MultiThreadedServiceFactory<DataStore> usingCredentials(final String apiKey) {
        final AbstractDataStoreClientFactory authServiceFactory = this;

        return new MultiThreadedServiceFactory<DataStore>() {
            @Override
            public String getServiceName() {
                return authServiceFactory.getServiceName();
            }

            @Override
            public void configure(ServicePoolBuilder<DataStore> servicePoolBuilder) {
                // Defaults are ok
            }

            @Override
            public DataStore create(ServiceEndPoint endPoint) {
                AuthDataStore authDataStore = authServiceFactory.create(endPoint);
                return new DataStoreAuthenticatorProxy(authDataStore, apiKey);
            }

            @Override
            public void destroy(ServiceEndPoint endPoint, DataStore service) {
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
