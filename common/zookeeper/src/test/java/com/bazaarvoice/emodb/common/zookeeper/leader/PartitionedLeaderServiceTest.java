package com.bazaarvoice.emodb.common.zookeeper.leader;

import com.bazaarvoice.curator.recipes.leader.LeaderService;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Service;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class PartitionedLeaderServiceTest {

    private TestingServer _zookeeper;

    @BeforeClass
    private void setupZookeeper() throws Exception {
        _zookeeper = new TestingServer();
    }

    @AfterClass
    private void teardownZookeeper() throws Exception {
        _zookeeper.close();
    }

    @Test
    public void testSingleServer() throws Exception {
        try (CuratorFramework curator = CuratorFrameworkFactory.newClient(_zookeeper.getConnectString(), new RetryNTimes(1, 10))) {
            curator.start();
            
            PartitionedLeaderService service = new PartitionedLeaderService(curator, "/leader/single", "instance0",
                    "test-service", 3, 1, 5000, TimeUnit.MILLISECONDS, TestLeaderService::new, null);

            service.start();

            List<LeaderService> leaderServices = service.getPartitionLeaderServices();
            assertEquals(leaderServices.size(), 3, "Wrong number of services for the provided number of partitions");

            // Give it 5 seconds to attain leadership on all 3 partitions
            Stopwatch stopwatch = Stopwatch.createStarted();
            Set<Integer> leadPartitions = getLeadPartitions(service);

            while (leadPartitions.size() != 3 && stopwatch.elapsed(TimeUnit.SECONDS) < 5) {
                Thread.sleep(10);
                leadPartitions = getLeadPartitions(service);
            }

            assertEquals(leadPartitions, ImmutableSet.of(0, 1, 2));
            service.stop();
        }
    }

    @Test
    public void testMultiServer() throws Exception {
        List<CuratorFramework> curators = Lists.newArrayListWithCapacity(3);

        try {
            List<PartitionedLeaderService> services = Lists.newArrayListWithCapacity(3);
            for (int i=0; i < 3; i++) {
                CuratorFramework curator = CuratorFrameworkFactory.newClient(_zookeeper.getConnectString(), new RetryNTimes(1, 10));
                curator.start();
                curators.add(curator);

                PartitionedLeaderService service = new PartitionedLeaderService(curator, "/leader/multi", "instance" + i,
                        "test-service", 10, 1, 5000, TimeUnit.MILLISECONDS, TestLeaderService::new, null);
                service.start();
                services.add(service);
            }


            // Give it 5 seconds for leadership to be balanced.  With 10 partitions and 3 services this should
            // be [4, 3, 3] since the first instance has the lowest instance ID.
            Stopwatch stopwatch = Stopwatch.createStarted();
            List<Set<Integer>> leadPartitions = ImmutableList.of();

            while (!leadPartitions.stream().map(Set::size).collect(Collectors.toList()).equals(ImmutableList.of(4, 3, 3)) && stopwatch.elapsed(TimeUnit.SECONDS) < 5) {
                Thread.sleep(10);
                leadPartitions = services.stream()
                        .map(this::getLeadPartitions)
                        .collect(Collectors.toList());
            }

            Set<Integer> unclaimedPartitions = Sets.newLinkedHashSet();
            for (int i=0; i < 10; i++) {
                unclaimedPartitions.add(i);
            }

            assertEquals(leadPartitions.size(), 3, "There should be three sets of partitions of leadership");
            assertEquals(leadPartitions.get(0).size(), 4);
            assertTrue(Sets.intersection(leadPartitions.get(0), unclaimedPartitions).equals(leadPartitions.get(0)));
            unclaimedPartitions.removeAll(leadPartitions.get(0));
            assertEquals(leadPartitions.get(1).size(), 3);
            assertTrue(Sets.intersection(leadPartitions.get(1), unclaimedPartitions).equals(leadPartitions.get(1)));
            unclaimedPartitions.removeAll(leadPartitions.get(1));
            assertEquals(leadPartitions.get(2).size(), 3);
            assertEquals(leadPartitions.get(2), unclaimedPartitions);

            for (PartitionedLeaderService service : services) {
                service.stop();
            }
        } finally {
            for (CuratorFramework curator : curators) {
                curator.close();
            }
        }
    }

    @Test
    public void testRelinquish() throws Exception {
        List<CuratorFramework> curators = Lists.newArrayListWithCapacity(2);

        try {
            List<PartitionedLeaderService> services = Lists.newArrayListWithCapacity(2);
            for (int i=0; i < 2; i++) {
                CuratorFramework curator = CuratorFrameworkFactory.newClient(_zookeeper.getConnectString(), new RetryNTimes(1, 10));
                curator.start();
                curators.add(curator);

                PartitionedLeaderService service = new PartitionedLeaderService(curator, "/leader/relinquish", "instance" + i,
                        "test-service", 2, 1, 5000, TimeUnit.MILLISECONDS, TestLeaderService::new, null);
                services.add(service);
            }

            // Start only the first service
            services.get(0).start();

            // Wait up to 5 seconds for the service to acquire leadership for both partitions
            Stopwatch stopwatch = Stopwatch.createStarted();
            while (getLeadPartitions(services.get(0)).size() != 2 && stopwatch.elapsed(TimeUnit.SECONDS) < 5) {
                Thread.sleep(10);
            }

            assertEquals(getLeadPartitions(services.get(0)).size(), 2, "Service should have acquired leadership for both partitions");

            // Now start the second service
            services.get(1).start();

            // Wait for the first service to relinquish one partition and for the second service to acquire it
            stopwatch.reset().start();
            Set<Integer> leadPartitions0 = ImmutableSet.of();
            Set<Integer> leadPartitions1 = ImmutableSet.of();

            while ((leadPartitions0.size() != 1 || leadPartitions1.size() != 1) && stopwatch.elapsed(TimeUnit.SECONDS) < 5) {
                Thread.sleep(10);
                leadPartitions0 = getLeadPartitions(services.get(0));
                leadPartitions1 = getLeadPartitions(services.get(1));
            }

            Set<Integer> leadPartitions = Sets.newLinkedHashSet();
            assertEquals(leadPartitions0.size(), 1);
            leadPartitions.addAll(leadPartitions0);
            assertEquals(leadPartitions1.size(), 1);
            leadPartitions.addAll(leadPartitions1);
            assertEquals(leadPartitions, ImmutableSet.of(0, 1));

            // Finally, kill the first service completely
            services.get(0).stop();

            // Wait for the second service to lead both partitions
            stopwatch.reset().start();
            leadPartitions1 = ImmutableSet.of();

            while (!leadPartitions1.equals(ImmutableSet.of(0, 1)) && stopwatch.elapsed(TimeUnit.SECONDS) < 5) {
                Thread.sleep(10);
                leadPartitions1 = getLeadPartitions(services.get(1));
            }

            assertEquals(leadPartitions1, ImmutableSet.of(0, 1));

            for (PartitionedLeaderService service : services) {
                service.stop();
            }
        } finally {
            for (CuratorFramework curator : curators) {
                curator.close();
            }
        }
    }

    private Set<Integer> getLeadPartitions(PartitionedLeaderService partitionedLeaderService) {
        Set<Integer> leadPartitions = Sets.newLinkedHashSet();

        for (Integer partition : partitionedLeaderService.getLeadingPartitions()) {
            // Don't just take the service's word for it; double check that the service is actually in the execution body
            Service uncastDelegate = partitionedLeaderService.getPartitionLeaderServices().get(partition).getCurrentDelegateService().orElse(null);
            TestLeaderService delegate = uncastDelegate != null && uncastDelegate instanceof TestLeaderService ?
                    (TestLeaderService) uncastDelegate : null;
            if (delegate != null && delegate.inBody) {
                leadPartitions.add(delegate.partition);
            }
        }

        return leadPartitions;
    }

    private class TestLeaderService extends AbstractExecutionThreadService {

        final int partition;
        volatile boolean inBody;

        public TestLeaderService(int partition) {
            this.partition = partition;
        }

        @Override
        protected void run() throws Exception {
            inBody = true;
            try {
                while (isRunning()) {
                    Thread.sleep(10);
                }
            } finally {
                inBody = false;
            }
        }
    }
}
