package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.databus.ChannelNames;
import com.bazaarvoice.emodb.databus.SystemIdentity;
import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.event.owner.OstrichOwnerFactory;
import com.bazaarvoice.emodb.event.owner.OstrichOwnerGroupFactory;
import com.bazaarvoice.emodb.event.owner.OwnerGroup;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.table.db.ClusterInfo;
import com.bazaarvoice.emodb.table.db.Placements;
import com.bazaarvoice.emodb.table.db.consistency.DatabusClusterInfo;
import com.bazaarvoice.ostrich.PartitionContext;
import com.bazaarvoice.ostrich.PartitionContextBuilder;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/** Starts the Databus canary threads, one for each SoR Cassandra cluster, subject to ZooKeeper leader election. */
public class CanaryManager {
    @Inject
    public CanaryManager(final LifeCycleRegistry lifeCycle,
                         @DatabusClusterInfo Collection<ClusterInfo> clusterInfo,
                         Placements placements,
                         final DatabusFactory databusFactory,
                         final @SystemIdentity String systemId,
                         final RateLimitedLogFactory logFactory,
                         OstrichOwnerGroupFactory ownerGroupFactory,
                         final MetricRegistry metricRegistry) {
        // Index the Cassandra cluster info objects by their name.
        final Map<String, ClusterInfo> clusterInfoMap = Maps.newLinkedHashMap();
        for (ClusterInfo cluster : clusterInfo) {
            clusterInfoMap.put(cluster.getCluster(), cluster);
        }

        // Figure out which placement strings belong to which cluster.
        Multimap<String, String> clusterToPlacements = ArrayListMultimap.create();
        for (String placement : placements.getValidPlacements()) {
            Optional<String> cluster = placements.getLocalCluster(placement);
            if (cluster.isPresent()) {
                clusterToPlacements.put(cluster.get(), placement);
            }
        }

        // Build a Databus Condition object that selects updates based on the underlying Cassandra cluster.
        final Map<String, Condition> clusterToConditionMap = Maps.newHashMap();
        for (Map.Entry<String, Collection<String>> entry : clusterToPlacements.asMap().entrySet()) {
            String cluster = entry.getKey();
            List<String> clusterPlacements = Ordering.natural().immutableSortedCopy(entry.getValue());
            checkState(clusterInfoMap.containsKey(cluster),
                    "Placement(s) map to unknown cluster '%s': %s", cluster, clusterPlacements);
            clusterToConditionMap.put(cluster,
                    Conditions.intrinsic(Intrinsic.PLACEMENT, Conditions.in(clusterPlacements)));
        }


        // Since the canary reads from the databus it must either (a) execute on the same server that owns
        // and manages the databus canary subscription or (b) go through the Ostrich client that forwards
        // requests to the right server.  This code implements the first option by using the same consistent
        // hash calculation used by Ostrich to determine which server owns the canary subscription.  As Emo
        // servers join and leave the pool, the OwnerGroup will track which server owns the canary subscription
        // at a given point in time and start and stop the canary service appropriately.
        OwnerGroup<Service> ownerGroup = ownerGroupFactory.create("Canary", new OstrichOwnerFactory<Service>() {
            @Override
            public PartitionContext getContext(String cluster) {
                return PartitionContextBuilder.of(ChannelNames.getMasterCanarySubscription(cluster));
            }

            @Override
            public Service create(String clusterName) {
                ClusterInfo cluster = checkNotNull(clusterInfoMap.get(clusterName), clusterName);
                Condition condition = checkNotNull(clusterToConditionMap.get(clusterName), clusterName);
                Databus databus = databusFactory.forOwner(systemId);
                return new Canary(cluster, condition, databus, logFactory, metricRegistry);
            }
        }, null);
        lifeCycle.manage(ownerGroup);

        // Start one canary for each Cassandra data cluster.
        for (String cluster : clusterToConditionMap.keySet()) {
            ownerGroup.startIfOwner(cluster, Duration.ZERO);
        }
    }
}
