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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closeables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestDefaultJobService {

    private QueueService _queueService;
    private DefaultJobHandlerRegistry _jobHandlerRegistry;
    private DefaultJobService _service;
    private TestingServer _testingServer;
    private CuratorFramework _curator;

    @BeforeMethod
    public void setUp() throws Exception {
        LifeCycleRegistry lifeCycleRegistry = mock(LifeCycleRegistry.class);
        _queueService = mock(QueueService.class);
        _jobHandlerRegistry = new DefaultJobHandlerRegistry();
        JobStatusDAO jobStatusDAO = new InMemoryJobStatusDAO();
        _testingServer = new TestingServer();
        _curator = CuratorFrameworkFactory.builder()
                .connectString(_testingServer.getConnectString())
                .retryPolicy(new RetryNTimes(3, 100))
                .build();

        _curator.start();

        _service = new DefaultJobService(
                lifeCycleRegistry, _queueService, "testqueue", _jobHandlerRegistry, jobStatusDAO, _curator,
                1, Duration.ZERO, 100, Duration.ofHours(1));
    }

    @AfterMethod
    public void shutDown() throws Exception {
        Closeables.close(_curator, true);
        Closeables.close(_testingServer, true);
    }

    @Test
    public void testRunOneJob() throws Exception {
        _jobHandlerRegistry.addHandler(new TestJobType(), Suppliers.ofInstance(
                new JobHandler<TestRequest, TestResult>() {
                    @Override
                    public TestResult run(TestRequest request)
                            throws Exception {
                        return new TestResult(Collections.nCopies(request.getValue2(), request.getValue1()));
                    }
                }));

        JobIdentifier<TestRequest, TestResult> jobId = submitJob(new TestRequest("hello", 3));
        runJob(jobId, true, true);

        JobStatus<TestRequest, TestResult> status = _service.getJobStatus(jobId);
        assertEquals(status.getStatus(), JobStatus.Status.FINISHED);
        assertEquals(status.getRequest(), new TestRequest("hello", 3));
        assertEquals(status.getResult(), new TestResult(ImmutableList.of("hello", "hello", "hello")));
    }

    @Test
    public void testRunOneJobWithFailure() {
        _jobHandlerRegistry.addHandler(new TestJobType(), Suppliers.ofInstance(
                new JobHandler<TestRequest, TestResult>() {
                    @Override
                    public TestResult run(TestRequest request)
                            throws Exception {
                        throw new IllegalArgumentException("Your argument is invalid");
                    }
                }));

        JobIdentifier<TestRequest, TestResult> jobId = submitJob(new TestRequest("hello", 3));
        runJob(jobId, true, true);

        JobStatus<TestRequest, TestResult> status = _service.getJobStatus(jobId);
        assertEquals(status.getStatus(), JobStatus.Status.FAILED);
        assertEquals(status.getRequest(), new TestRequest("hello", 3));
        assertNull(status.getResult());
        assertEquals(status.getErrorMessage(), "Your argument is invalid");
    }

    @Test
    public void testRunOneJobNonLocal() {
        _jobHandlerRegistry.addHandler(new TestJobType(), Suppliers.ofInstance(
                new JobHandler<TestRequest, TestResult>() {
                    @Override
                    public TestResult run(TestRequest request)
                            throws Exception {
                        return notOwner();
                    }
                }));

        JobIdentifier<TestRequest, TestResult> jobId = submitJob(new TestRequest("hello", 3));

        // Job will run, but it won't be local so it won't be acked.
        runJob(jobId, true, false);

        // Job won't run because it's known to be running on a non-local server.
        runJob(jobId, false, false);
    }

    private JobIdentifier<TestRequest, TestResult> submitJob(TestRequest request) {
        JobIdentifier<TestRequest, TestResult> jobId =
                _service.submitJob(new JobRequest<>(new TestJobType(), request));

        verify(_queueService, times(1)).send("testqueue", jobId.toString());

        return jobId;
    }

    private void runJob(JobIdentifier<TestRequest, TestResult> jobId, boolean runExpected, boolean ackExpected) {
        JobStatus<TestRequest, TestResult> status = _service.getJobStatus(jobId);
        assertEquals(status.getStatus(), JobStatus.Status.SUBMITTED);
        assertEquals(status.getRequest(), new TestRequest("hello", 3));
        assertNull(status.getResult());

        when(_queueService.peek(eq("testqueue"), anyInt())).thenReturn(
                ImmutableList.of(
                        new Message("12345", jobId.toString())
                ),
                ImmutableList.of()
        );

        boolean ran = _service.runNextJob();
        assertEquals(ran, runExpected);

        verify(_queueService, ackExpected ? times(1) : never()).acknowledge("testqueue", ImmutableList.of("12345"));
    }

    public final static class TestRequest {
        private final String _value1;
        private final int _value2;

        @JsonCreator
        public TestRequest(@JsonProperty("value1") String value1, @JsonProperty("value2") Integer value2) {
            _value1 = value1;
            _value2 = value2;
        }

        public String getValue1() {
            return _value1;
        }

        public int getValue2() {
            return _value2;
        }

        public boolean equals(Object other) {
            return other instanceof TestRequest &&
                    Objects.equal(((TestRequest) other)._value1, _value1) &&
                    ((TestRequest) other)._value2 == _value2;
        }
    }

    public final static class TestResult {
        private final List<String> _values;

        @JsonCreator
        public TestResult(@JsonProperty("values") List<String> values) {
            _values = values;
        }

        public List<String> getValues() {
            return _values;
        }

        public boolean equals(Object other) {
            return other instanceof TestResult &&
                    Objects.equal(((TestResult) other)._values, _values);
        }

    }

    public final static class TestJobType extends JobType<TestRequest, TestResult> {
        public TestJobType() {
            super("test", TestRequest.class, TestResult.class);
        }
    }
}
