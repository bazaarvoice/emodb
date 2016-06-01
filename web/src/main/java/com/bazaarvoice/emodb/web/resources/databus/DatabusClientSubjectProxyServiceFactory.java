package com.bazaarvoice.emodb.web.resources.databus;

import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.databus.api.AuthDatabus;
import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.databus.client.DatabusAuthenticator;
import com.bazaarvoice.ostrich.MultiThreadedServiceFactory;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Service factory representation that proxies an {@link AuthDatabus} service factory with a
 * {@link DatabusClientSubjectProxy} by using the subject's ID as the AuthDatabus credential.
 */
public class DatabusClientSubjectProxyServiceFactory implements MultiThreadedServiceFactory<DatabusClientSubjectProxy> {

    private final MultiThreadedServiceFactory<AuthDatabus> _authDatabusServiceFactory;

    public DatabusClientSubjectProxyServiceFactory(MultiThreadedServiceFactory<AuthDatabus> authDatabusServiceFactory) {
        _authDatabusServiceFactory = checkNotNull(authDatabusServiceFactory);
    }

    @Override
    public String getServiceName() {
        return _authDatabusServiceFactory.getServiceName();
    }

    @Override
    public void configure(ServicePoolBuilder<DatabusClientSubjectProxy> servicePoolBuilder) {
        // No need to configure, this class proxies to a pre-configured AuthDatabus service factory.
    }

    @Override
    public DatabusClientSubjectProxy create(ServiceEndPoint endPoint) {
        AuthDatabus authDatabus = _authDatabusServiceFactory.create(endPoint);
        return new RemoteDatabusClientSubjectProxy(authDatabus);
    }

    @Override
    public void destroy(ServiceEndPoint endPoint, DatabusClientSubjectProxy service) {
        RemoteDatabusClientSubjectProxy databusFactory = (RemoteDatabusClientSubjectProxy) service;
        _authDatabusServiceFactory.destroy(endPoint, databusFactory.getAuthDatabus());
    }

    @Override
    public boolean isHealthy(ServiceEndPoint endPoint) {
        return _authDatabusServiceFactory.isHealthy(endPoint);
    }

    @Override
    public boolean isRetriableException(Exception exception) {
        return _authDatabusServiceFactory.isRetriableException(exception);
    }

    private static class RemoteDatabusClientSubjectProxy implements DatabusClientSubjectProxy {
        private final AuthDatabus _authDatabus;
        private final DatabusAuthenticator _databusAuthenticator;

        private RemoteDatabusClientSubjectProxy(AuthDatabus authDatabus) {
            _authDatabus = authDatabus;
            _databusAuthenticator = DatabusAuthenticator.proxied(authDatabus);
        }

        @Override
        public Databus forSubject(Subject subject) {
            // Use the database authenticator based on the subject's external ID, which is their API key
            return _databusAuthenticator.usingCredentials(subject.getId());
        }

        public AuthDatabus getAuthDatabus() {
            return _authDatabus;
        }
    }
}
