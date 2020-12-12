package com.bazaarvoice.emodb.job.service;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.job.api.JobHandler;
import com.bazaarvoice.emodb.job.api.JobIdentifier;
import com.bazaarvoice.emodb.job.api.JobRequest;
import com.bazaarvoice.emodb.job.api.JobStatus;
import com.bazaarvoice.emodb.job.api.JobType;
import com.bazaarvoice.emodb.job.dao.InMemoryJobStatusDAO;
import com.bazaarvoice.emodb.job.dao.JobStatusDAO;
import com.bazaarvoice.emodb.job.handler.DefaultJobHandlerRegistry;
import com.bazaarvoice.emodb.queue.api.Message;
import com.bazaarvoice.emodb.queue.api.QueueService;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.io.Closeables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestJobOwnership {
    private JobStatusDAO _jobStatusDAO;
    private TestingServer _testingServer;
    private CuratorFramework _curator;

    private DefaultJobHandlerRegistry _jobHandlerRegistry1;
    private DefaultJobHandlerRegistry _jobHandlerRegistry2;
    private DefaultJobService _service1;
    private DefaultJobService _service2;

    private final AtomicInteger _nextMessageId = new AtomicInteger(1);
    private final Deque<Message> _queue = Queues.newArrayDeque();

    @BeforeMethod
    public void setUp() throws Exception {
        LifeCycleRegistry lifeCycleRegistry = mock(LifeCycleRegistry.class);
        QueueService queueService = mock(QueueService.class);

        when(queueService.peek(eq("testqueue"), anyInt())).thenAnswer(new Answer<List<Message>>() {
            @Override
            public List<Message> answer(InvocationOnMock invocationOnMock) {
                return ImmutableList.copyOf(Iterables.limit(_queue, (Integer) invocationOnMock.getArguments()[1]));
            }
        });

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock)
                    throws Throwable {
                _queue.add(new Message(
                        String.valueOf(_nextMessageId.getAndIncrement()),
                        invocationOnMock.getArguments()[1]));
                return null;
            }
        }).when(queueService).send(eq("testqueue"), any());

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock)
                    throws Throwable {
                List<String> messageIds = (List<String>) invocationOnMock.getArguments()[1];
                for (Iterator<Message> messages = _queue.iterator(); messages.hasNext(); ) {
                    Message message = messages.next();
                    if (messageIds.contains(message.getId())) {
                        messages.remove();
                    }
                }
                return null;
            }
        }).when(queueService).acknowledge(eq("testqueue"), anyList());

        _jobStatusDAO = new InMemoryJobStatusDAO();
        _testingServer = new TestingServer();
        _curator = CuratorFrameworkFactory.builder()
                .connectString(_testingServer.getConnectString())
                .retryPolicy(new RetryNTimes(3, 100))
                .build();

        _curator.start();

        _jobHandlerRegistry1 = new DefaultJobHandlerRegistry();
        _jobHandlerRegistry2 = new DefaultJobHandlerRegistry();
        _service1 = new DefaultJobService(
                lifeCycleRegistry, queueService, "testqueue", _jobHandlerRegistry1, _jobStatusDAO, _curator,
                1, Duration.ZERO, 100, Duration.ofHours(1));

        _service2 = new DefaultJobService(
                lifeCycleRegistry, queueService, "testqueue", _jobHandlerRegistry2, _jobStatusDAO, _curator,
                1, Duration.ZERO, 100, Duration.ofHours(1));
    }

    @AfterMethod
    public void shutDown() throws Exception {
        Closeables.close(_curator, true);
        Closeables.close(_testingServer, true);
    }

    /**
     * Tests that jobs that can run on any server will be executed by all servers.
     */
    @Test
    public void testRunsAnywhere() throws Exception {
        JobType<String, String> type = new JobType<String, String>("test", String.class, String.class) {};

        // Create a job handler that transforms the request string
        Supplier<JobHandler<String, String>> handlers = new Supplier<JobHandler<String, String>>() {
            @Override
            public JobHandler<String, String> get() {
                return new JobHandler<String, String>() {
                    @Override
                    public String run(String request)
                            throws Exception {
                        return "[" + request + "]";
                    }
                };
            }
        };

        // Register the handler with both registries (and, by extension, both JobServices).
        _jobHandlerRegistry1.addHandler(type, handlers);
        _jobHandlerRegistry2.addHandler(type, handlers);

        List<JobIdentifier<String, String>> jobIds = Lists.newArrayList();

        // Create 10 of the jobs submitted only to service 1.  Since service1 and service2 share a queue all jobs
        // should be visible to both.
        for (int i = 0; i < 10; i++) {
            JobIdentifier<String, String> jobId = _service1.submitJob(new JobRequest<>(type, String.format("test-%d", i)));
            jobIds.add(jobId);
        }

        // Run all 10 jobs one at a time, alternating between service1 and service2
        for (int i = 0; i < 10; i++) {
            boolean jobRan = i % 2 == 0 ? _service1.runNextJob() : _service2.runNextJob();
            assertTrue(jobRan);
        }

        // Verify both services are fully drained
        assertFalse(_service1.runNextJob());
        assertFalse(_service2.runNextJob());

        // Validate all 10 jobs ran successfully
        for (int i = 0; i < 10; i++) {
            JobIdentifier<String, String> jobId = jobIds.get(i);
            JobStatus<String, String> status = _jobStatusDAO.getJobStatus(jobId);
            assertNotNull(status);
            assertEquals(status.getStatus(), JobStatus.Status.FINISHED);
            assertEquals(status.getRequest(), String.format("test-%d", i));
            assertEquals(status.getResult(), String.format("[test-%d]", i));
        }
    }

    /**
     * Tests that jobs that can only run on a specific server will only run on that server.
     */
    @Test
    public void testRunsWithLocalOwnership() throws Exception {
        JobType<String, String> type = new JobType<String, String>("test", String.class, String.class) {};

        // Create a job handler for service1 that will only accept jobs whose requests are prefaced with "1".
        Supplier<JobHandler<String, String>> handlers1 = new Supplier<JobHandler<String, String>>() {
            @Override
            public JobHandler<String, String> get() {
                return new JobHandler<String, String>() {
                    @Override
                    public String run(String request)
                            throws Exception {
                        if (!request.startsWith("1")) {
                            return notOwner();
                        }
                        return request + "-1";
                    }
                };
            }
        };

        // Similarly, create a job handler for service2 that will only accept jobs whose requests are prefaced with "2".
        Supplier<JobHandler<String, String>> handlers2 = new Supplier<JobHandler<String, String>>() {
            @Override
            public JobHandler<String, String> get() {
                return new JobHandler<String, String>() {
                    @Override
                    public String run(String request)
                            throws Exception {
                        if (!request.startsWith("2")) {
                            return notOwner();
                        }
                        return request + "-2";
                    }
                };
            }
        };

        // Register the handlers to their respective registries (and, by extension, JobServices).
        _jobHandlerRegistry1.addHandler(type, handlers1);
        _jobHandlerRegistry2.addHandler(type, handlers2);

        List<JobIdentifier<String, String>> service1Ids = Lists.newArrayList();
        List<JobIdentifier<String, String>> service2Ids = Lists.newArrayList();

        // Create 20 jobs, 10 that can only be handled by service1 and 10 that can only be handled by service2
        for (int i = 0; i < 10; i++) {
            JobIdentifier<String, String> jobId = _service1.submitJob(new JobRequest<>(type, String.format("1-%d-test", i)));
            service1Ids.add(jobId);
            jobId = _service1.submitJob(new JobRequest<>(type, String.format("2-%d-test", i)));
            service2Ids.add(jobId);
        }

        // Create two threads to drain each JobService's queues.
        CountDownLatch startDraining = new CountDownLatch(1);
        AtomicBoolean stopDraining = new AtomicBoolean(false);
        CountDownLatch queueEmpty = new CountDownLatch(2);

        startDrainerThread(_service1, startDraining, queueEmpty, stopDraining);
        startDrainerThread(_service2, startDraining, queueEmpty, stopDraining);

        // Signal the threads to start draining
        startDraining.countDown();

        // Give both JobServices 10 seconds to drain the queues.
        queueEmpty.await(10, TimeUnit.SECONDS);

        // Signal the threads to stop draining
        stopDraining.set(true);

        // Validate that all jobs ran and that they ran on the correct server.  This can be validated because server1's
        // and server2's handlers each generate unique responses.
        for (int i = 0; i < 10; i++) {
            for (int s = 1; s <= 2; s++) {
                JobIdentifier<String, String> jobId = (s == 1 ? service1Ids : service2Ids).get(i);
                JobStatus<String, String> status = _jobStatusDAO.getJobStatus(jobId);
                assertNotNull(status);
                assertEquals(status.getStatus(), JobStatus.Status.FINISHED);
                assertEquals(status.getRequest(), String.format("%d-%d-test", s, i));
                assertEquals(status.getResult(), String.format("%d-%d-test-%d", s, i, s));
            }
        }
    }

    private Thread startDrainerThread(final DefaultJobService service, final CountDownLatch startDraining,
                                      final CountDownLatch queueEmpty, final AtomicBoolean stopDraining) {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    startDraining.await();

                    while (!stopDraining.get()) {
                        boolean jobRan = service.runNextJob();
                        if (!jobRan) {
                            queueEmpty.countDown();
                            return;
                        }
                    }
                } catch (InterruptedException e) {
                    // Stop processing on an interrupt
                }
            }
        });

        thread.start();
        return thread;
    }
}
