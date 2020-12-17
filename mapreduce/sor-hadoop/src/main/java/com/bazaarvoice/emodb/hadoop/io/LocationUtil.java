package com.bazaarvoice.emodb.hadoop.io;

import com.bazaarvoice.curator.ResolvingEnsembleProvider;
import com.bazaarvoice.emodb.sor.client.DataStoreClient;
import com.bazaarvoice.emodb.sor.client.DataStoreFixedHostDiscoverySource;
import com.bazaarvoice.ostrich.HostDiscovery;
import com.bazaarvoice.ostrich.discovery.zookeeper.ZooKeeperHostDiscovery;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Optional.ofNullable;

/**
 * Locations take take one of the following forms (all starting with "emodb://"):
 * <p>
 * ci.us/tablename
 * ci.us.group/tablename
 * ci.us/tablename?zkConnectString=zookeeper-connect-string
 * ci.us/tablename?host=hostname
 * <p>
 * For local host:
 * <p>
 * local/tablename
 * local.group/tablename
 * local/tablename?zkConnectString=zookeeper-connect-string
 * local/tablename?host=hostname
 * <p>
 * Alternately the caller can provide a direct emodb URL, such as "emodb://localhost:8080/tablename"
 */
public class LocationUtil {
    private static final String DEFAULT_ZK_CONNECTION_STRING = "zookeeper1:2181,zookeeper2:2181,zookeeper3:2181";
    private static final String DEFAULT_LOCAL_ZK_CONNECTION_STRING = "localhost:2181";
    private static final String DEFAULT_REGION = "us-east-1";
    private static final String DEFAULT_GROUP = "default";

    private static final String EMODB_SCHEME = "emodb";
    private static final String STASH_SCHEME = "emostash";
    private static final String ZK_CONNECTION_STRING_PARAM = "zkConnectionString";
    private static final String HOST_PARAM = "host";

    private static final Pattern LOCATOR_PATTERN = Pattern.compile(
            "^((?<universe>ci|cert|anon|uat|bazaar)(\\.(?<region>us|us-east-1|eu|eu-west-1))?|local)" +
                    "(\\.(?<group>.+?))?$");

    // Supported regions
    private static enum Region {
        us("us-east-1", "us"),
        eu("eu-west-1", "eu");

        private final String _defaultAlias;
        private final Set<String> _aliases;

        private Region(String... aliases) {
            _defaultAlias = aliases[0];
            _aliases = ImmutableSet.copyOf(aliases);
        }

        public Set<String> getAliases() {
            return _aliases;
        }

        public String toString() {
            return _defaultAlias;
        }
    }

    private static Region getRegion(String value) {
        for (Region region : Region.values()) {
            if (region.getAliases().contains(value)) {
                return region;
            }
        }
        throw new IllegalArgumentException("Invalid region " + value);
    }

    /**
     * Returns the location type from a location URI.
     */
    public static LocationType getLocationType(URI location) {
        String scheme = location.getScheme();
        if (EMODB_SCHEME.equals(scheme)) {
            // If the host matches the locator pattern then assume host discovery, otherwise it's a
            // direct URL to an EmoDB server
            if (LOCATOR_PATTERN.matcher(location.getHost()).matches()) {
                return LocationType.EMO_HOST_DISCOVERY;
            } else {
                return LocationType.EMO_URL;
            }
        } else if (STASH_SCHEME.equals(scheme)) {
            return LocationType.STASH;
        }

        throw new IllegalArgumentException("Invalid location: " + location);
    }

    private static Matcher getLocatorMatcher(URI location) {
        return LOCATOR_PATTERN.matcher(location.getHost());
    }

    /**
     * Converts a location URI to a data source identifier.
     */
    public static String getDataStoreIdentifier(URI location, String apiKey) {
        checkArgument(getLocationType(location) != LocationType.STASH, "Stash locations do not have a data source ID");

        UriBuilder uriBuilder = UriBuilder.fromUri(location)
                .userInfo(apiKey)
                .replacePath(null)
                .replaceQuery(null);

        if (getLocationType(location) == LocationType.EMO_HOST_DISCOVERY) {
            Optional<String> zkConnectionStringOverride = getZkConnectionStringOverride(location);
            if (zkConnectionStringOverride.isPresent()) {
                uriBuilder.queryParam(ZK_CONNECTION_STRING_PARAM, zkConnectionStringOverride.get());
            }
            Optional<List<String>> hosts = getHostOverride(location);
            if (hosts.isPresent()) {
                for (String host : hosts.get()) {
                    uriBuilder.queryParam(HOST_PARAM, host);
                }
            }
        }
        return uriBuilder.build().toString();
    }

    public static Optional<String> getZkConnectionStringOverride(URI location) {
        List<NameValuePair> params = URLEncodedUtils.parse(location, Charsets.UTF_8.name());
        for (NameValuePair pair : params) {
            if (ZK_CONNECTION_STRING_PARAM.equals(pair.getName())) {
                // Deterministically sort the hosts
                String[] hosts = pair.getValue().split(",");
                Arrays.sort(hosts);
                String zkConnectionString = Joiner.on(",").join(hosts);
                return Optional.of(zkConnectionString);
            }
        }
        return Optional.absent();
    }

    public static URI setZkConnectionStringOverride(URI location, String zkConnectionString) {
        return UriBuilder.fromUri(location)
                .replaceQueryParam(ZK_CONNECTION_STRING_PARAM, zkConnectionString)
                .build();
    }

    public static Optional<List<String>> getHostOverride(URI location) {
        List<NameValuePair> params = URLEncodedUtils.parse(location, Charsets.UTF_8.name());
        List<String> hosts = FluentIterable.from(params)
                .filter(new Predicate<NameValuePair>() {
                    public boolean apply(NameValuePair pair) {
                        return HOST_PARAM.equals(pair.getName());
                    }
                })
                .transform(new Function<NameValuePair, String>() {
                    public String apply(NameValuePair pair) {
                        return pair.getValue();
                    }
                })
                .toSortedList(Ordering.natural());

        return hosts.isEmpty() ? Optional.<List<String>>absent() : Optional.of(hosts);
    }

    public static URI setHostsOverride(URI location, String... hosts) {
        return UriBuilder.fromUri(location)
                .replaceQueryParam(HOST_PARAM, hosts)
                .build();
    }

    /**
     * Returns a configured, started Curator for a given location, or absent if the location does not
     * use host discovery.
     */
    public static Optional<CuratorFramework> getCuratorForLocation(URI location) {
        final String defaultConnectionString;
        final String namespace;

        if (getLocationType(location) != LocationType.EMO_HOST_DISCOVERY) {
            // Only host discovery may require ZooKeeper
            return Optional.absent();
        }

        if (getHostOverride(location).isPresent()) {
            // Fixed host discovery doesn't require ZooKeeper
            return Optional.absent();
        }

        Matcher matcher = getLocatorMatcher(location);
        checkArgument(matcher.matches(), "Invalid location: %s", location);

        if (matcher.group("universe") != null) {
            // Normal host discovery
            String universe = matcher.group("universe");
            Region region = getRegion(ofNullable(matcher.group("region")).orElse(DEFAULT_REGION));
            namespace = format("%s/%s", universe, region);
            defaultConnectionString = DEFAULT_ZK_CONNECTION_STRING;
        } else {
            // Local host discovery; typically for developer testing
            namespace = null;
            defaultConnectionString = DEFAULT_LOCAL_ZK_CONNECTION_STRING;
        }

        String connectionString = getZkConnectionStringOverride(location).or(defaultConnectionString);

        CuratorFramework curator = CuratorFrameworkFactory.builder()
                .ensembleProvider(new ResolvingEnsembleProvider(connectionString))
                .retryPolicy(new BoundedExponentialBackoffRetry(100, 1000, 10))
                .threadFactory(new ThreadFactoryBuilder().setNameFormat("emo-zookeeper-%d").build())
                .namespace(namespace)
                .build();

        curator.start();

        return Optional.of(curator);
    }

    /**
     * Returns the EmoDB cluster name associated with the given location.
     */
    public static String getClusterForLocation(URI location) {
        Matcher matcher = getLocatorMatcher(location);
        checkArgument(matcher.matches(), "Invalid location: %s", location);

        final String clusterPrefix;

        if (matcher.group("universe") != null) {
            clusterPrefix = matcher.group("universe");
        } else {
            clusterPrefix = "local";
        }

        String group = ofNullable(matcher.group("group")).orElse(DEFAULT_GROUP);
        return format("%s_%s", clusterPrefix, group);
    }

    /**
     * Returns the HostDiscovery for a given location.  The curator should come from a previous call to
     * {@link #getCuratorForLocation(java.net.URI)} to the same location.
     */
    public static HostDiscovery getHostDiscoveryForLocation(URI location, Optional<CuratorFramework> curator, String serviceName, MetricRegistry metricRegistry) {
        checkArgument(getLocationType(location) == LocationType.EMO_HOST_DISCOVERY, "Location does not use host discovery");
        Optional<List<String>> hosts = getHostOverride(location);
        if (hosts.isPresent()) {
            return createFixedHostDiscovery(serviceName, hosts.get().toArray(new String[hosts.get().size()]));
        } else {
            checkState(curator.isPresent(), "curator required");
            return createZooKeeperHostDiscovery(curator.get(), serviceName, metricRegistry);
        }
    }

    private static HostDiscovery createZooKeeperHostDiscovery(CuratorFramework curator, String serviceName, MetricRegistry metricRegistry) {
        return new ZooKeeperHostDiscovery(curator, serviceName, metricRegistry);
    }

    private static HostDiscovery createFixedHostDiscovery(String serviceName, String... hosts) {
        return new DataStoreFixedHostDiscoverySource(hosts).forService(serviceName);
    }

    /**
     * Returns the base URI for a location of type {@link LocationType#EMO_URL}.
     */
    public static URI getBaseUriForLocation(URI location) {
        // Any request to the public ELBs must be made over https
        boolean useHttps = location.getHost().toLowerCase().endsWith("nexus.bazaarvoice.com");

        return UriBuilder.fromUri(location)
                .scheme(useHttps ? "https" : "http")
                .replacePath(DataStoreClient.SERVICE_PATH)
                .replaceQuery(null)
                .build();
    }

    /**
     * Returns a location URI from a source and table name.
     */
    public static URI toLocation(String source, String table) {
        // Verify the source is a valid EmoDB URI
        URI sourceUri = URI.create(source);
        return toLocation(sourceUri, table);
    }

    /**
     * Returns a Location URI from a source URI and table name.
     */
    public static URI toLocation(URI sourceUri, String table) {
        getLocationType(sourceUri); // Raises an exception if the location type is invalid.
        return UriBuilder.fromUri(sourceUri).path(table).build();
    }

    /**
     * Returns location information for a given location of type {@link LocationType#STASH}.
     */
    public static StashLocation getStashLocation(URI location) {
        checkArgument(getLocationType(location) == LocationType.STASH, "Not a stash location");

        String host, path;
        boolean useLatestDirectory;

        Matcher matcher = getLocatorMatcher(location);
        if (matcher.matches()) {
            // Stash is only available for the default group.  Make sure that's the case here
            String group = ofNullable(matcher.group("group")).orElse(DEFAULT_GROUP);
            checkArgument(DEFAULT_GROUP.equals(group), "Stash not available for group: %s", group);

            // Stash it not available for local
            String universe = matcher.group("universe");
            checkArgument(universe != null, "Stash not available for local");

            Region region = getRegion(ofNullable(matcher.group("region")).orElse(DEFAULT_REGION));

            host = getS3BucketForRegion(region);
            path = getS3PathForUniverse(universe);
            useLatestDirectory = true;
        } else {
            // The location refers to the actual S3 bucket and path
            host = location.getHost();
            path = location.getPath();
            // Stash paths don't have a leading slash
            if (path != null && path.startsWith("/")) {
                path = path.substring(1);
            }
            useLatestDirectory = false;
        }

        return new StashLocation(host, path, useLatestDirectory);
    }

    private static String getS3BucketForRegion(Region region) {
        return format("emodb-%s", region);
    }

    private static String getS3PathForUniverse(String universe) {
        return format("stash/%s", universe);
    }
}
