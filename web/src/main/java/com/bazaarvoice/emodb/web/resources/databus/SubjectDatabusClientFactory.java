package com.bazaarvoice.emodb.web.resources.databus;

import com.bazaarvoice.emodb.databus.api.AuthDatabus;
import com.bazaarvoice.emodb.databus.client.DatabusClient;
import com.bazaarvoice.ostrich.MultiThreadedServiceFactory;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.partition.ConsistentHashPartitionFilter;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Service factory for a SubjectDatabus.  Implementation is a thin wrapper around an {@link AuthDatabus}
 * service factory.  Client implementations returned by the delegated factory are wrapped by a
 * {@link SubjectDatabusClient} to allow authentication using the subject's API key.
 *
 * Note that in order for this delegation strategy to work the {@link com.bazaarvoice.ostrich.partition.PartitionKey}
 * annotations in {@link AbstractSubjectDatabus} must exactly match those in {@link DatabusClient}, otherwise
 * subscriptions won't be forwarded to the correct server.
 */
public class SubjectDatabusClientFactory implements MultiThreadedServiceFactory<SubjectDatabus> {

    private final MultiThreadedServiceFactory<AuthDatabus> _authDatabusFactory;

    public SubjectDatabusClientFactory(MultiThreadedServiceFactory<AuthDatabus> authDatabusFactory) {
        _authDatabusFactory = checkNotNull(authDatabusFactory, "authDatabusFactory");
    }

    @Override
    public String getServiceName() {
        return _authDatabusFactory.getServiceName();
    }

    @Override
    public void configure(ServicePoolBuilder<SubjectDatabus> servicePoolBuilder) {
        servicePoolBuilder.withPartitionFilter(new ConsistentHashPartitionFilter())
                .withPartitionContextAnnotationsFrom(AbstractSubjectDatabus.class);
    }

    @Override
    public SubjectDatabus create(ServiceEndPoint endPoint) {
        return new SubjectDatabusClient(_authDatabusFactory.create(endPoint));
    }

    @Override
    public void destroy(ServiceEndPoint endPoint, SubjectDatabus service) {
        _authDatabusFactory.destroy(endPoint, ((SubjectDatabusClient) service).getClient());
    }

    @Override
    public boolean isHealthy(ServiceEndPoint endPoint) {
        return _authDatabusFactory.isHealthy(endPoint);
    }

    @Override
    public boolean isRetriableException(Exception exception) {
        return _authDatabusFactory.isRetriableException(exception);
    }
}
