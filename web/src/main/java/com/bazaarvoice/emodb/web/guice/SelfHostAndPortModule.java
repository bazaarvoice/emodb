package com.bazaarvoice.emodb.web.guice;

import com.bazaarvoice.emodb.common.dropwizard.guice.SelfAdminHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.dropwizard.jetty.ConnectorFactory;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.server.DefaultServerFactory;
import io.dropwizard.server.ServerFactory;
import io.dropwizard.server.SimpleServerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Exports the following Guice beans:
 * <ol>
 * <li> @{@link SelfHostAndPort} {@link HostAndPort}
 * <li> @{@link SelfAdminHostAndPort} {@link HostAndPort}
 * </ol>
 * Requires the following external references:
 * <ol>
 * <li> Dropwizard {@link ServerFactory}
 * </ol>
 */
public class SelfHostAndPortModule extends AbstractModule {
    @Override
    protected void configure() {
    }

    @Provides
    @Singleton
    @SelfHostAndPort
    public HostAndPort provideSelfHostAndPort(ServerFactory serverFactory) {
        // Our method for obtaining connector factories from the server factory varies depending on the latter's type
        List<ConnectorFactory> appConnectorFactories;
        if (serverFactory instanceof DefaultServerFactory) {
            appConnectorFactories = ((DefaultServerFactory) serverFactory).getApplicationConnectors();
        } else if (serverFactory instanceof SimpleServerFactory) {
            appConnectorFactories = Collections.singletonList(((SimpleServerFactory) serverFactory).getConnector());
        } else {
            throw new IllegalStateException("Encountered an unexpected ServerFactory type");
        }

        return getHostAndPortFromConnectorFactories(appConnectorFactories);
    }

    @Provides
    @Singleton
    @SelfAdminHostAndPort
    public HostAndPort provideSelfAdminHostAndPort(ServerFactory serverFactory) {
        // Our method for obtaining connector factories from the server factory varies depending on the latter's type
        List<ConnectorFactory> adminConnectorFactories;
        if (serverFactory instanceof DefaultServerFactory) {
            adminConnectorFactories = ((DefaultServerFactory) serverFactory).getAdminConnectors();
        } else if (serverFactory instanceof SimpleServerFactory) {
            adminConnectorFactories = Collections.singletonList(((SimpleServerFactory) serverFactory).getConnector());
        } else {
            throw new IllegalStateException("Encountered an unexpected ServerFactory type");
        }

        return getHostAndPortFromConnectorFactories(adminConnectorFactories);
    }

    private static HostAndPort getHostAndPortFromConnectorFactories(List<ConnectorFactory> connectors) {
        // find the first connector that matches and return it host/port information (in practice there should
        // be one, and just one, match)
        try {
            HttpConnectorFactory httpConnectorFactory = (HttpConnectorFactory) Iterables.find(connectors, Predicates.instanceOf(HttpConnectorFactory.class));

            String host = httpConnectorFactory.getBindHost();
            if (host == null) {
                host = getLocalHost();
            }
            int port = httpConnectorFactory.getPort();
            return HostAndPort.fromParts(host, port);
        } catch (NoSuchElementException ex) {
            throw new IllegalStateException("Did not find a valid HttpConnector for the server", ex);
        }
    }

    private static String getLocalHost() {
        final String localHostEnvVariableName = "LOCAL_HOST";
        String localHost = System.getenv(localHostEnvVariableName);

        if (null == localHost || localHost.trim().isEmpty()) {
            try {
                localHost = InetAddress.getLocalHost().getHostAddress();
            } catch (IOException e) {
                throw new AssertionError(e); // Should never happen
            }
        }

        return localHost;
    }
}
