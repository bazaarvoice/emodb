package com.bazaarvoice.emodb.table.db;

import com.google.common.base.MoreObjects;

import static com.google.common.base.Preconditions.checkNotNull;

public class ClusterInfo {
    private final String _cluster;
    private final String _clusterMetric;

    public ClusterInfo(String cluster, String clusterMetric) {
        _cluster = checkNotNull(cluster, "cluster");
        _clusterMetric = MoreObjects.firstNonNull(clusterMetric, cluster);
    }

    public String getCluster(){
        return _cluster;
    }

    public String getClusterMetric() {
        return _clusterMetric;
    }
}
