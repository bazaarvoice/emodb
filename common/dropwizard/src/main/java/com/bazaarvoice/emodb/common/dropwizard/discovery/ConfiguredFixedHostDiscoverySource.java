package com.bazaarvoice.emodb.common.dropwizard.discovery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

/**
 * An SOA (Ostrich) helper class that can be used to configure a {@link com.bazaarvoice.ostrich.ServicePool}
 * with a fixed, hard-coded set of hosts, useful for testing and for cross-data center API calls where
 * the client and EmoDB servers aren't in the same data center and don't have access to the same ZooKeeper
 * ensemble.
 * <p>
 * To use, reference this class from your Dropwizard configuration and configure it in your YAML file.  If
 * the YAML file doesn't specify any end points, the {@code ServicePool} will fall back to using ZooKeeper.
 * Otherwise, the {@code ServicePool} will ignore ZooKeeper and use only the end points specified in YAML.
 * <p>
 * Example Dropwizard {@code Configuration} additions:
 * <pre>
 * public class MyAppConfiguration extends Configuration {
 *     ...
 *     public MySvcFixedHostDiscoverySource endPointOverrides = new MySvcFixedHostDiscoverySource();
 * }
 * </pre>
 * Reference the configuration class when constructing the {@link com.bazaarvoice.ostrich.ServicePool}:
 * <pre>
 * // Connect to the remote MyService using ZooKeeper/Ostrich host discovery.
 * MyService service = ServicePoolBuilder.create(MyService.class)
 *         .withHostDiscoverySource(configuration.endPointOverrides)
 *         .withZooKeeperHostDiscovery(zooKeeper)
 *         .withServiceFactory(new MyServiceClientFactory(jerseyClient))
 *         .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));
 * environment.manage(new ManagedServicePoolProxy(service));
 * </pre>
 * Example YAML that load balances between servers running on default ports in Amazon:
 * <pre>
 * endPointOverrides:
 *     ec2-184-72-181-173.compute-1.amazonaws.com: {}
 *     ec2-75-101-213-222.compute-1.amazonaws.com: {}
 * </pre>
 * You can specify non-standard configuration settings using the setter methods in {@link ConfiguredPayload}.  Example YAML:
 * <pre>
 * endPointOverrides:
 *     local-server-1:
 *         host:        localhost
 *         port:        8080
 *         adminPort:   8081
 *     local-server-2:
 *         host:        localhost
 *         port:        8090
 *         adminPort:   8091
 * </pre>
 */
public abstract class ConfiguredFixedHostDiscoverySource
        extends com.bazaarvoice.ostrich.discovery.ConfiguredFixedHostDiscoverySource<ConfiguredPayload> {

    public ConfiguredFixedHostDiscoverySource() {
        super();
    }

    public ConfiguredFixedHostDiscoverySource(String... hosts) {
        super(toDefaultEndpoints(hosts));
    }

    @JsonCreator
    public ConfiguredFixedHostDiscoverySource(Map<String, ConfiguredPayload> endPoints) {
        super(endPoints);
    }

    /**
     * Returns the base name of the service.
     */
    protected abstract String getBaseServiceName();

    /**
     * Returns the url path to the service, usually the value of the {@link @Path} annotation on the Jersey resource
     * of the service implementation.
     */
    protected abstract String getServicePath();

    @Override
    protected String serialize(String serviceName, String id, ConfiguredPayload payload) {
        checkState(ServiceNames.isValidServiceName(serviceName, getBaseServiceName()));
        return payload.toPayload(id, getServicePath()).toString();
    }

    private static Map<String, ConfiguredPayload> toDefaultEndpoints(String... hosts) {
        ImmutableMap.Builder<String, ConfiguredPayload> builder = ImmutableMap.builder();
        for (String host : hosts) {
            // Currently ostrich has a bug that doesn't accept valid url's with '/' in them
            // So, any hosts with '/' in their url are rejected.
            // This poses an issue when ELB's are used and "https://" has to be specified
            // For now, we will check if scheme is identified, and treat it specially.

            // Check if scheme ("http://" or "https://" is specified), otherwise the default is applied

            String scheme = "http";
            if (host.startsWith("http://") || host.startsWith("https://")) {
                // Extract the scheme
                int colonPos = host.indexOf(':');
                scheme = host.substring(0, colonPos);
                host = host.substring(colonPos + 3);
            }

            builder.put(host, new ConfiguredPayload().withScheme(scheme));
        }
        return builder.build();
    }
}
