package com.bazaarvoice.emodb.databus.client2.discovery;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import static java.util.Objects.requireNonNull;

/**
 * Service discovery implementation which directs requests to a spectific host based on a partition key.  The algorithm
 * comes directly from EmoDB and Zookeeper.
 */
public class PartitionedDiscovery extends ZKEmoServiceDiscovery {

    private final int _partitionHash;
    private volatile URI _partitionedUri;

    public PartitionedDiscovery(String zookeeperConnectionString, String service, String partitionKey) {
        super(zookeeperConnectionString, service);
        Hasher hasher = Hashing.md5().newHasher();
        putUnencodedChars(hasher, partitionKey);
        _partitionHash = hasher.hash().asInt();
    }

    @Override
    protected void hostsChanged(List<Host> sortedHosts) {
        NavigableMap<Integer, URI> ring = Maps.newTreeMap();
        for (Host host : sortedHosts) {
            for (Integer hash : computeHashCodes(host.id)) {
                ring.put(hash, host.baseUri);
            }
        }
        Map.Entry<Integer, URI> entry = ring.ceilingEntry(_partitionHash);
        if (entry == null) {
            entry = ring.firstEntry();
        }
        if (entry != null) {
            _partitionedUri = entry.getValue();
        } else {
            _partitionedUri = null;
        }
    }

    @Override
    protected URI getBaseUriFromDiscovery() {
        return _partitionedUri;
    }

    private List<Integer> computeHashCodes(String id) {
        List<Integer> list = Lists.newArrayListWithCapacity(100);
        for (int i = 0; list.size() < 100; i++) {
            Hasher hasher = Hashing.md5().newHasher();
            hasher.putInt(i);
            putUnencodedChars(hasher, id);
            ByteBuffer buf = ByteBuffer.wrap(hasher.hash().asBytes());
            while (buf.hasRemaining() && list.size() < 100) {
                list.add(buf.getInt());
            }
        }
        return list;
    }

    private void putUnencodedChars(Hasher hasher, String str) {
        int len = str.length();
        for (int i = 0; i < len; i++) {
            hasher.putChar(str.charAt(i));
        }
    }

    public static class Builder extends ZKEmoServiceDiscovery.Builder {
        private String _subscription;

        Builder(String cluster) {
            super(requireNonNull(cluster, "Cluster is required") + "-emodb-bus-1");
        }

        @Override
        public Builder withZookeeperDiscovery(String zookeeperConnectionString, String zookeeperNamespace) {
            super.withZookeeperDiscovery(zookeeperConnectionString, zookeeperNamespace);
            return this;
        }

        public Builder withSubscription(String subscription) {
            _subscription = subscription;
            return this;
        }

        public void validate() {
            super.validate();
            if (Strings.isNullOrEmpty(_subscription)) {
                throw new IllegalStateException("A valid subscription is required");
            }
        }

        public PartitionedDiscovery build() {
            validate();
            return new PartitionedDiscovery(
                    getZookeeperConnectionString(), getService(), _subscription);
        }
    }

}
