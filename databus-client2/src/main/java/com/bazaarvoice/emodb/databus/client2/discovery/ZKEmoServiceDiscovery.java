package com.bazaarvoice.emodb.databus.client2.discovery;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.AbstractService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static com.bazaarvoice.emodb.databus.client2.Json.JsonUtil.parseJson;
import static com.google.common.base.Preconditions.checkArgument;

public abstract class ZKEmoServiceDiscovery extends AbstractService implements EmoServiceDiscovery {

    private static final Logger _log = LoggerFactory.getLogger(ZKEmoServiceDiscovery.class);

    private final String _zookeeperConnectionString;
    private final String _service;

    private volatile CuratorFramework _rootCurator;
    private volatile CuratorFramework _curator;
    private volatile PathChildrenCache _pathCache;

    protected ZKEmoServiceDiscovery(String zookeeperConnectionString, String service) {
        _zookeeperConnectionString = zookeeperConnectionString;
        _service = service;
    }

    @Override
    protected void doStart() {
        if (_zookeeperConnectionString != null) {
            try {
                _log.debug("Zookeeper config {} ", _zookeeperConnectionString);
                startNodeListener();
            } catch (Exception e) {
                _log.error("Exception while trying to start NodeListener {}", e);
                doStop();
                throw Throwables.propagate(e);
            }
        }

        notifyStarted();
    }

    @Override
    protected void doStop() {
        try {
            _log.debug("Closing zookeeper pathCache... ");
            if (_pathCache != null) {
                Closeables.close(_pathCache, true);
                _pathCache = null;
            }
            if (_rootCurator != null) {
                Closeables.close(_rootCurator, true);
                _rootCurator = _curator = null;
            }
        } catch (IOException ignore) {
            // Already managed
        }

        notifyStopped();
    }

    private void startNodeListener() throws Exception {
        String path = ZKPaths.makePath("ostrich", _service);
        _pathCache = new PathChildrenCache(_curator, path, true);
        _pathCache.getListenable().addListener((curator, event) -> rebuildHosts());
        _pathCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        rebuildHosts();
    }

    @Override
    public URI getBaseUri() throws UnknownHostException {
        URI uri = getBaseUriFromDiscovery();
        if (uri == null) {
            throw new UnknownHostException("No hosts discovered");
        }
        return uri;
    }

    private void rebuildHosts() {
        List<ChildData> currentData = _pathCache.getCurrentData();
        List<ZKEmoServiceDiscovery.Host> hosts = Lists.newArrayListWithCapacity(currentData.size());
        _log.info("Total no. of hosts {}, host:{} ", currentData.size(),
                currentData.size() > 0 ? hosts.get(0).baseUri : 0);
        for (ChildData childData : currentData) {
            ZKEmoServiceDiscovery.RegistrationData registrationData = parseJson(childData.getData(), ZKEmoServiceDiscovery.RegistrationData.class);
            ZKEmoServiceDiscovery.PayloadData payloadData = parseJson(registrationData.payload, ZKEmoServiceDiscovery.PayloadData.class);
            URI baseUri = UriBuilder.fromUri(payloadData.serviceUrl).replacePath(null).build();
            hosts.add(new ZKEmoServiceDiscovery.Host(registrationData.id, baseUri));
        }
        Collections.sort(hosts);
        hostsChanged(hosts);
    }

    abstract protected void hostsChanged(List<ZKEmoServiceDiscovery.Host> sortedHosts);

    abstract protected URI getBaseUriFromDiscovery();

    /**
     * The actual ZooKeeper registration contains more attributes than this, but we'll only keep the ones which matter
     * for service location.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class RegistrationData {
        public String id;
        public String payload;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class PayloadData {
        public String serviceUrl;
    }

    protected static class Host implements Comparable<ZKEmoServiceDiscovery.Host> {
        public final String id;
        public final URI baseUri;

        public Host(String id, URI baseUri) {
            this.id = id;
            this.baseUri = baseUri;
        }

        @Override
        public int compareTo(ZKEmoServiceDiscovery.Host o) {
            return id.compareTo(o.id);
        }
    }

    abstract protected static class Builder implements Serializable {
        private final String _service;
        private String _zookeeperConnectionString;
        private String _zookeeperNamespace;

        public Builder(String service) {
            _service = Objects.requireNonNull(service, "Service name is required");
        }

        public ZKEmoServiceDiscovery.Builder withZookeeperDiscovery(String zookeeperConnectionString, String zookeeperNamespace) {
            checkArgument(zookeeperConnectionString != null || zookeeperNamespace == null, "Connection string is required");
            _zookeeperConnectionString = zookeeperConnectionString;
            _zookeeperNamespace = zookeeperNamespace;
            return this;
        }

        protected String getService() {
            return _service;
        }

        protected String getZookeeperConnectionString() {
            return _zookeeperConnectionString;
        }

//        TODO this is still unused, do we miss something?
        protected String getZookeeperNamespace() {
            return _zookeeperNamespace;
        }

        protected void validate() {
            if (_zookeeperConnectionString == null) {
                throw new IllegalStateException("zookeeperConnectionString is required");
            }
        }
    }
}
