package com.bazaarvoice.emodb.web.purge;

import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.job.api.JobIdentifier;
import com.bazaarvoice.emodb.job.api.JobRequest;
import com.bazaarvoice.emodb.job.api.JobStatus;
import com.bazaarvoice.emodb.job.api.JobType;
import com.bazaarvoice.emodb.job.dao.InMemoryJobStatusDAO;
import com.bazaarvoice.emodb.job.dao.JobStatusDAO;
import com.bazaarvoice.emodb.job.handler.DefaultJobHandlerRegistry;
import com.bazaarvoice.emodb.job.service.DefaultJobService;
import com.bazaarvoice.emodb.queue.api.QueueService;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.core.DefaultDataStoreAsync;
import com.bazaarvoice.emodb.sor.core.PurgeJob;
import com.bazaarvoice.emodb.sor.core.PurgeRequest;
import com.bazaarvoice.emodb.sor.core.PurgeResult;
import com.bazaarvoice.emodb.sor.core.test.InMemoryDataStore;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.web.auth.Permissions;
import com.bazaarvoice.emodb.web.resources.sor.AuditParam;
import com.bazaarvoice.emodb.web.resources.sor.DataStoreResource1;
import com.bazaarvoice.emodb.web.throttling.UnlimitedDataStoreUpdateThrottler;
import com.codahale.metrics.MetricRegistry;
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
import java.util.Map;
import java.util.Objects;

import static com.bazaarvoice.emodb.job.api.JobIdentifier.createNew;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;



public class PurgeTest {
    private static final String TABLE = "testtable";
    private static final String TABLE2 = "testtable2";
    private static final String TABLE3 = "testtable3";
    private static final String KEY1 = "key1";
    private static final String KEY2 = "key2";

    private QueueService _queueService;
    private DefaultJobHandlerRegistry _jobHandlerRegistry;
    private JobStatusDAO _jobStatusDAO;
    private DefaultJobService _service;
    private TestingServer _testingServer;
    private CuratorFramework _curator;

    public DataStore _store;
    public DataStoreResource1 _dataStoreResource;

    @BeforeMethod
    public void setUp() throws Exception {
        LifeCycleRegistry lifeCycleRegistry = mock(LifeCycleRegistry.class);
        _queueService = mock(QueueService.class);
        _jobHandlerRegistry = new DefaultJobHandlerRegistry();
        _jobStatusDAO = new InMemoryJobStatusDAO();
        _testingServer = new TestingServer();
        _curator = CuratorFrameworkFactory.builder()
                .connectString(_testingServer.getConnectString())
                .retryPolicy(new RetryNTimes(3, 100))
                .build();

        _curator.start();

        _service = new DefaultJobService(
                lifeCycleRegistry, _queueService, "testqueue", _jobHandlerRegistry, _jobStatusDAO, _curator,
                1, Duration.ZERO, 100, Duration.ofHours(1));

        _store = new InMemoryDataStore(new MetricRegistry());
        _dataStoreResource = new DataStoreResource1(_store, new DefaultDataStoreAsync(_store, _service, _jobHandlerRegistry),
                mock(CompactionControlSource.class), new UnlimitedDataStoreUpdateThrottler());

    }

    @AfterMethod
    public void shutDown() throws Exception {
        Closeables.close(_curator, true);
        Closeables.close(_testingServer, true);
    }

    @Test
    public void serviceTest() {

        // check can instantiate job type
        JobRequest testPurgeJobRequest = new JobRequest<>(PurgeJob.INSTANCE, new PurgeRequest("test", newAudit("new job test")));
        assertTrue(Objects.equals(testPurgeJobRequest.getType().getName(), "purge"));

        //checking for PurgeJob registration
        JobType jobType = testPurgeJobRequest.getType();
        JobIdentifier newJobID = createNew(jobType);
        checkNotNull(newJobID, "jobId");

        // submitting a job to registry purge drops the table
        JobIdentifier jobID = _service.submitJob(new JobRequest<>(PurgeJob.INSTANCE, new PurgeRequest(TABLE, newAudit("submitting purge job directly via submitJob"))));
        JobStatus<PurgeRequest, PurgeResult> status = _service.getJobStatus(jobID);
        assertTrue(status.getStatus().toString().equals("SUBMITTED"));
    }

    @Test
    public void resourceTest() {

        TableOptions options = new TableOptionsBuilder().setPlacement("default").build();
        _store.createTable(TABLE, options, Collections.<String,Object >emptyMap(), newAudit("create table"));
        _store.createTable(TABLE2, options, Collections.<String,Object >emptyMap(), newAudit("create table"));
        _store.createTable(TABLE3, options, Collections.<String,Object >emptyMap(), newAudit("create table"));
        //checking tables has been created
        assertTrue(_store.getTableExists(TABLE));
        assertTrue(_store.getTableExists(TABLE2));

        // write some data
        _store.update(TABLE, KEY1, TimeUUIDs.newUUID(), Deltas.fromString("{\"name\":\"Bob\"}"), newAudit("submit"), WriteConsistency.STRONG);
        _store.update(TABLE, KEY1, TimeUUIDs.newUUID(), Deltas.fromString("{..,\"state\":\"SUBMITTED\"}"), newAudit("begin moderation"), WriteConsistency.STRONG);
        _store.update(TABLE, KEY2, TimeUUIDs.newUUID(), Deltas.fromString("{\"name\":\"Joe\"}"), newAudit("submit"), WriteConsistency.STRONG);
        assertTrue(_store.getTableExists(TABLE2));

        // unit test for purgeTableUnsafe
        _store.purgeTableUnsafe(TABLE2, newAudit("checking purgeTableUnsafe"));
        assertTrue(_store.getTableApproximateSize(TABLE2) == 0L);

        // submitting a job to registry purge drops the table
        JobIdentifier jobID = _service.submitJob(new JobRequest<>(PurgeJob.INSTANCE, new PurgeRequest(TABLE, newAudit("submitting purge job directly via submitJob"))));
        Map<String, Object> purgeStatus = _dataStoreResource.getPurgeStatus(TABLE, jobID.toString());
        assertTrue(purgeStatus.get("status").toString().equals("IN_PROGRESS"));

        // calling job directly via calling purgeTableAsync on DataStoreResource1
        Map<String, Object> jobID3Map = _dataStoreResource.purgeTableAsync(TABLE3, new AuditParam("audit=comment:'purgetest+comment'"));
        Map<String, Object> purgeStatus3 = _dataStoreResource.getPurgeStatus(TABLE3, jobID3Map.get("id").toString());
        assertTrue(purgeStatus3.get("status").toString().equals("IN_PROGRESS"));
    }

    private Audit newAudit(String comment) {
        return new AuditBuilder().
                setProgram("test").
                setUser("root").
                setLocalHost().
                setComment(comment).
                build();
    }
}
