package com.bazaarvoice.emodb.table.db.consistency;

import com.bazaarvoice.curator.recipes.leader.LeaderService;
import com.bazaarvoice.emodb.common.cassandra.cqldriver.HintsPollerCQLSession;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ServiceFailureListener;
import com.bazaarvoice.emodb.common.zookeeper.store.ValueStore;
import com.bazaarvoice.emodb.table.db.astyanax.CQLSessionForHintsPollerMap;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.Session;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.Managed;
import org.apache.curator.framework.CuratorFramework;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Starts a Hints Poller service for each cluster, subject ZooKeeper leader election.
 */
public class HintsPollerManager implements Managed {

    private LifeCycleRegistry _lifeCycle;
    private final Map<String, ValueStore<Long>> _timestampCache;
    private CuratorFramework _curator;
    private HostAndPort _self;
    private final Map<String, HintsPollerCQLSession> _cqlSessionForHintsPollerMap;
    private final ClusterHintsPoller _clusterHintsPoller;
    private LeaderServiceTask _dropwizardTask;
    private final MetricRegistry _metricRegistry;
    private List<LeaderService> _leaderServiceList;

    @Inject
    public HintsPollerManager(LifeCycleRegistry lifeCycle,
                              @HintsConsistencyTimeValues final Map<String, ValueStore<Long>> timestampCache,
                              @GlobalFullConsistencyZooKeeper CuratorFramework curator,
                              @SelfHostAndPort HostAndPort self,
                              @CQLSessionForHintsPollerMap final Map<String, HintsPollerCQLSession> cqlSessionForHintsPollerMap,
                              final ClusterHintsPoller clusterHintsPoller,
                              LeaderServiceTask dropwizardTask,
                              final MetricRegistry metricRegistry) {
        _lifeCycle = lifeCycle;
        _timestampCache = checkNotNull(timestampCache, "timestampCache");
        _curator = checkNotNull(curator, "curator");
        _self = checkNotNull(self, "self");
        _cqlSessionForHintsPollerMap = checkNotNull(cqlSessionForHintsPollerMap, "cqlSessionForHintsPollerMap");
        _clusterHintsPoller = checkNotNull(clusterHintsPoller, "clusterHintsPoller");
        _dropwizardTask = checkNotNull(dropwizardTask, "dropwizardTask");
        _metricRegistry = checkNotNull(metricRegistry, "metricRegistry");

        _lifeCycle.manage(this);
    }

    public void start()
            throws Exception {
        _leaderServiceList = getLeaderServices(_timestampCache, _curator, _self, _cqlSessionForHintsPollerMap, _clusterHintsPoller, _dropwizardTask, _metricRegistry);
        for (LeaderService leaderService : _leaderServiceList) {
            leaderService.startAsync();
            leaderService.awaitRunning();
        }
    }


    public void stop()
            throws Exception {
        for (LeaderService leaderService : _leaderServiceList) {
            leaderService.stopAsync();
            leaderService.awaitTerminated();
        }
    }

    public List<LeaderService> getLeaderServices(final Map<String, ValueStore<Long>> timestampCache,
                                                 CuratorFramework curator,
                                                 HostAndPort self,
                                                 final Map<String, HintsPollerCQLSession> cqlSessionForHintsPollerMap,
                                                 final ClusterHintsPoller clusterHintsPoller,
                                                 LeaderServiceTask dropwizardTask,
                                                 final MetricRegistry metricRegistry) {
        _leaderServiceList = Lists.newArrayList();
        String serverId = self.toString(); // For debugging

        // Start one hints poller for each data store cluster
        for (final Map.Entry<String, HintsPollerCQLSession> entry : cqlSessionForHintsPollerMap.entrySet()) {
            final String clusterName = entry.getKey();
            final Session session = entry.getValue().getCqlSession();
            String zkLeaderPath = "/leader/hints/" + clusterName;
            String threadName = "Leader-HintsPoller-" + clusterName;
            LeaderService leaderService = new LeaderService(
                    curator, zkLeaderPath, serverId, threadName, 1, TimeUnit.MINUTES,
                    () -> new HintsPollerService(clusterName, timestampCache.get(clusterName), session, clusterHintsPoller, metricRegistry));
            ServiceFailureListener.listenTo(leaderService, metricRegistry);
            dropwizardTask.register("hints-" + clusterName, leaderService);
            _leaderServiceList.add(leaderService);
        }

        return _leaderServiceList;
    }
}
