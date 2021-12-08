package com.bazaarvoice.emodb.web.jetty;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dropwizard.jetty.HttpConnectorFactory;
import org.eclipse.jetty.io.ByteBufferPool;
import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.thread.Scheduler;
import org.eclipse.jetty.util.thread.ThreadPool;

import static com.codahale.metrics.MetricRegistry.name;

@JsonTypeName ("instrumentedHttp")
public class InstrumentedHttpConnectorFactory extends HttpConnectorFactory {

    // Why make the MetricRegistry thread-local? In order to obey the HttpConnectorFactory interface's contract, any calls that are made to build() should result in a
    // InstrumentedConnectionFactoryWrapper that reports metrics to the same MetricRegistry that was passed into build(). Normally we would support this by simply passing that MetricRegistry instance
    // down the stack, but unfortunately the buildConnector() method we build off of does not support that parameter. We must save off the registry in build() and manually retrieve it further down the
    // stack. This is where trouble appears - if two threads call build() at the same time with different registries, then the last to be supplied will effectively "win" and be used for both
    // connectors. Using a thread-local version guards against this - we guarantee that buildConnector will only use the registry supplied to its build() call in the same thread.
    private ThreadLocal<MetricRegistry> _metricRegistry = new ThreadLocal<>();

    @Override
    public Connector build(Server server,
                           MetricRegistry metrics,
                           String name,
                           ThreadPool threadPool) {
        _metricRegistry.set(metrics);
        return super.build(server, metrics, name, threadPool);
    }

    @Override
    protected ServerConnector buildConnector(Server server,
                                             Scheduler scheduler,
                                             ByteBufferPool bufferPool,
                                             String name,
                                             ThreadPool threadPool,
                                             ConnectionFactory... factories) {
        // Intercept any buildConnector() calls and wrap the provided ConnectionFactory instances with our InstrumentedConnectionFactoryWrapper
        InstrumentedConnectionFactoryWrapper connectionFactoryWrappers[] = new InstrumentedConnectionFactoryWrapper[factories.length];
        for (int i = 0; i < factories.length; ++i) {
            connectionFactoryWrappers[i] = new InstrumentedConnectionFactoryWrapper(factories[i], _metricRegistry.get(), getBindHost(), getPort());
        }

        return super.buildConnector(server, scheduler, bufferPool, name, threadPool, connectionFactoryWrappers);
    }

    // Wrapper class to encapsulate an existing ConnectionFactory and add our own listener to any connections it creates. This listener will update a count
    // of the active connections being made at any given time.
    private static class InstrumentedConnectionFactoryWrapper implements ConnectionFactory {

        private final ConnectionFactory _wrappedConnectionFactory;
        private final Counter _activeConnectionCounter;

        public InstrumentedConnectionFactoryWrapper(ConnectionFactory wrappedConnectionFactory, MetricRegistry metricRegistry, String bindHost, Integer port) {
            _wrappedConnectionFactory = wrappedConnectionFactory;

            final String counterName = name(HttpConnectionFactory.class,
                    bindHost,
                    Integer.toString(port),
                    "activeConnections");
            _activeConnectionCounter = metricRegistry.counter(counterName);
        }

        @Override
        public String getProtocol() {
            return _wrappedConnectionFactory.getProtocol();
        }

        @Override
        public Connection newConnection(Connector connector, EndPoint endPoint) {
            // Allow the wrapper to build its connection, then add our own listener to it. This listener will update the metric for our active connection count.
            final Connection connection = _wrappedConnectionFactory.newConnection(connector, endPoint);
            connection.addListener(new Connection.Listener() {
                @Override
                public void onOpened(Connection connection) {
                    _activeConnectionCounter.inc();
                }

                @Override
                public void onClosed(Connection connection) {
                    _activeConnectionCounter.dec();
                }
            });
            return connection;
        }
    }
}
