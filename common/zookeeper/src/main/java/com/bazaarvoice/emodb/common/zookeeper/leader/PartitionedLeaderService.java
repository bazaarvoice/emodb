package com.bazaarvoice.emodb.common.zookeeper.leader;

import com.bazaarvoice.curator.recipes.leader.LeaderService;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import io.dropwizard.lifecycle.Managed;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * There are circumstances where a service has multiple partitions and it is desirable to balance the workers for these
 * partitions across the cluster.  {@link LeaderService} works for a single partition but doesn't allow for balancing
 * leadership for multiple related leaders such as in our partitioned case.  This class uses multiple LeaderServices
 * and makes a best effort to balance leadership across the cluster.
 */
public class PartitionedLeaderService implements Managed {

    private static final Logger _log = LoggerFactory.getLogger(PartitionedLeaderService.class);

    private static final String LEADER_PATH_PATTERN = "partition-%03d";

    private final CuratorFramework _curator;
    private final String _basePath;
    private final String _instanceId;
    private final String _serviceName;
    private final int _numPartitions;
    private final long _reacquireDelay;
    private final long _repartitionDelay;
    private final TimeUnit _delayUnit;
    private final Clock _clock;
    private final List<PartitionLeader> _partitionLeaders;

    private PartitionedServiceSupplier _serviceFactory;
    private PathChildrenCache _participantsCache;
    private volatile int _maxPartitionsLeader;

    public PartitionedLeaderService(CuratorFramework curator, String leaderPath, String instanceId, String serviceName,
                                    int numPartitions, long reacquireDelay, long repartitionDelay, TimeUnit delayUnit,
                                    PartitionedServiceSupplier serviceFactory, @Nullable Clock clock) {
        this(curator, leaderPath, instanceId, serviceName, numPartitions, reacquireDelay, repartitionDelay, delayUnit, clock);
        setServiceFactory(serviceFactory);
    }

    /**
     * Constructor for subclasses.  The subclasses should call {@link #setServiceFactory(PartitionedServiceSupplier)}
     * in their constructor.
     */
    protected PartitionedLeaderService(CuratorFramework curator, String leaderPath, String instanceId, String serviceName,
                                                  int numPartitions, long reacquireDelay, long repartitionDelay, TimeUnit delayUnit,
                                                  @Nullable Clock clock) {
        _curator = curator;
        _basePath = leaderPath;
        _instanceId = instanceId;
        _serviceName = serviceName;
        _numPartitions = numPartitions;
        _reacquireDelay = reacquireDelay;
        _repartitionDelay = repartitionDelay;
        _delayUnit = delayUnit;
        _clock = Optional.ofNullable(clock).orElse(Clock.systemUTC());

        _partitionLeaders = Lists.newArrayListWithCapacity(_numPartitions);
        for (int partition = 0; partition < _numPartitions; partition++) {
            _partitionLeaders.add(new PartitionLeader(partition));
        }
    }

    final protected void setServiceFactory(PartitionedServiceSupplier serviceFactory) {
        requireNonNull(serviceFactory, "serviceFactory");
        if (_serviceFactory != null) {
            throw new IllegalStateException("Service factory has already been set");
        }
        _serviceFactory = serviceFactory;
    }

    @Override
    public void start() throws Exception {
        if (_serviceFactory == null) {
            throw new IllegalStateException("Cannot start service without first providing a service factory");
        }
        
        String participantsPath = ZKPaths.makePath(_basePath, String.format(LEADER_PATH_PATTERN, 0));

        // Watch the participants for partition 0 to determine how many potential leaders there are total
        _participantsCache = new PathChildrenCache(_curator, participantsPath, true);
        _participantsCache.getListenable().addListener((ignore1, ignore2) -> participantsUpdated(false));
        _participantsCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        participantsUpdated(true);
        
        List<Future<Void>> futures = Lists.newArrayListWithCapacity(_numPartitions);

        for (PartitionLeader partitionLeader : _partitionLeaders) {
            futures.add(partitionLeader.startAsync());
        }

        for (Future<Void> future : futures) {
            future.get();
        }
    }

    @Override
    public void stop() throws Exception {
        _participantsCache.close();

        List<Future<Void>> futures = Lists.newArrayListWithCapacity(_numPartitions);
        for (PartitionLeader partitionLeader : _partitionLeaders) {
            futures.add(partitionLeader.stopAsync());
        }

        for (Future<Void> future : futures) {
            future.get();
        }
    }

    /**
     * Returns the underlying leader services for each partition.  The services are guaranteed to be returned
     * ordered by partition number.
     */
    public List<LeaderService> getPartitionLeaderServices() {
        return _partitionLeaders.stream()
                .map(PartitionLeader::getLeaderService)
                .collect(Collectors.toList());
    }

    private synchronized void participantsUpdated(boolean initialUpdate) {
        List<String> participantInstanceIds = _participantsCache.getCurrentData().stream()
                .map(participant -> new String(participant.getData(), Charsets.UTF_8))
                .sorted()
                .collect(Collectors.toList());

        if (participantInstanceIds.isEmpty()) {
            // On the initial update there may be no participants if this is the only instance in the cluster.
            // This is because the cache is started before the leader services.  For all but the initial update
            // this should never happen since this instance at minimum is a participant.
            if (!initialUpdate) {
                _log.warn("Participant cache returned no participants");
            }
            participantInstanceIds = ImmutableList.of(_instanceId);
        }

        // If the partitions cannot be split evenly among the participants then the following uses the sorted
        // participant instance IDs to deterministically allocate which instances get the extras.
        int numParticipants = participantInstanceIds.size();
        _maxPartitionsLeader = _numPartitions / numParticipants;
        if (participantInstanceIds.indexOf(_instanceId) < _numPartitions % numParticipants) {
            _maxPartitionsLeader += 1;
        }

        //
        List<Integer> leadingPartitions = getLeadingPartitions();
        int excessLeadership = leadingPartitions.size() - _maxPartitionsLeader;

        while (excessLeadership > 0 && !leadingPartitions.isEmpty()) {
            // This service owns more that it should given the new topology.  Voluntarily release the excess.
            int leadingPartitionsIndex = (int) Math.floor(Math.random() * leadingPartitions.size());
            int partitionToRelease = leadingPartitions.remove(leadingPartitionsIndex);
            PartitionLeader partitionLeader = _partitionLeaders.get(partitionToRelease);

            if (partitionLeader.relinquishLeadership()) {
                excessLeadership -= 1;
            }
        }
    }

    public List<Integer> getLeadingPartitions() {
        List<Integer> leadingPartitions = Lists.newArrayListWithCapacity(_numPartitions);
        for (PartitionLeader partitionLeader : _partitionLeaders) {
            if (partitionLeader.hasLeadership()) {
                leadingPartitions.add(partitionLeader._partition);
            }
        }
        return leadingPartitions;
    }

    /**
     * Inner class to maintain leadership information about a single partition.
     */
    private class PartitionLeader {

        private final int _partition;
        private final LeaderService _leaderService;
        private final SettableFuture<Void> _startedFuture = SettableFuture.create();
        private final SettableFuture<Void> _terminatedFuture = SettableFuture.create();
        private Instant _partitionReacquireTimeout = Instant.EPOCH;
        private int _partitionReacquireRejections = 0;

        PartitionLeader(int partition) {
            _partition = partition;

            String leaderPath = ZKPaths.makePath(_basePath, String.format(LEADER_PATH_PATTERN, partition));

            _leaderService = new LeaderService(_curator, leaderPath, _instanceId, _serviceName,
                    _reacquireDelay, _delayUnit, this::leadershipAcquired);

            _leaderService.addListener(new Service.Listener() {
                @Override
                public void running() {
                    _startedFuture.set(null);
                }

                @Override
                public void terminated(Service.State from) {
                    _terminatedFuture.set(null);
                }
            }, MoreExecutors.newDirectExecutorService());
        }

        Future<Void> startAsync() {
            _leaderService.startAsync();
            return _startedFuture;
        }

        Future<Void> stopAsync() {
            _leaderService.stopAsync();
            return _terminatedFuture;
        }

        Service leadershipAcquired() {
            synchronized(PartitionedLeaderService.this) {
                List<Integer> leadingPartitions = getLeadingPartitions();
                //noinspection SuspiciousMethodCalls
                leadingPartitions.remove((Object) _partition);

                if (leadingPartitions.size() < _maxPartitionsLeader || _partitionReacquireRejections >= 3) {
                    if (_partitionReacquireRejections >= 3) {
                        _log.warn("Accepting leadership of partition {} for {} because no other server became leader after {} rejections",
                                _partition, _serviceName, _partitionReacquireRejections);
                    } else {
                        _log.debug("Accepting leadership of partition {} for {}", _partition, _serviceName);
                    }

                    _partitionReacquireTimeout = Instant.EPOCH;
                    _partitionReacquireRejections = 0;

                    return _serviceFactory.createForPartition(_partition);
                } else {
                    if (_clock.instant().isAfter(_partitionReacquireTimeout)) {
                        _log.info("Rejecting leadership of partition {} for {}, already own partitions {}",
                                _partition, _serviceName, leadingPartitions);
                        _partitionReacquireTimeout = _clock.instant().plusMillis(_delayUnit.toMillis(_repartitionDelay));
                        _partitionReacquireRejections += 1;
                    } else {
                        _log.info("Rejecting leadership of partition {} for {} while in partition reacquire delay",
                                _partition, _serviceName, leadingPartitions);
                    }

                    // Return a service which will immediately relinquish leadership
                    return new RelinquishService();
                }
            }
        }

        boolean hasLeadership() {
            if (!_leaderService.isRunning() || !_leaderService.hasLeadership()) {
                return false;
            }
            // It's possible we technically have leadership but are in the process of giving it up because we already
            // are leading the maximum number of partitions.  Verify that this isn't a rejected service.
            Service delegateService = _leaderService.getCurrentDelegateService().orNull();
            return delegateService == null || !(delegateService instanceof RelinquishService);
        }
        
        boolean relinquishLeadership() {
            if (hasLeadership()) {
                // Release leadership by stopping the delegate service, but do not stop the leadership service itself
                Service delegateService = _leaderService.getCurrentDelegateService().orNull();
                if (delegateService != null) {
                    _log.info("Relinquishing leadership of partition {} for {}", _partition, _serviceName);
                    delegateService.stopAsync();
                }
                return true;
            } else {
                return false;
            }
        }

        LeaderService getLeaderService() {
            return _leaderService;
        }
    }

    /**
     * Service implementation which immediately returns having taken no action.  Used when the LeaderService gives
     * a partition leadership when the total number of partitions already lead is at the maximum.
     */
    private static class RelinquishService extends AbstractExecutionThreadService {
        @Override
        protected void run() throws Exception {
            // Do nothing
        }
    }
}
