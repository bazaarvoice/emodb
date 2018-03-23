package com.bazaarvoice.emodb.job.dao;

import com.bazaarvoice.emodb.common.dropwizard.guice.SystemTablePlacement;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.job.api.JobIdentifier;
import com.bazaarvoice.emodb.job.api.JobStatus;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import javax.annotation.Nullable;
import java.util.Map;

import static com.bazaarvoice.emodb.job.util.JobStatusUtil.narrow;
import static com.google.common.base.Preconditions.checkNotNull;

public class DataStoreJobStatusDAO implements JobStatusDAO {

    private final Supplier<String> _tableNameSupplier;
    private final DataStore _dataStore;

    @Inject
    public DataStoreJobStatusDAO(@JobsTableName final Supplier<String> tableNameSupplier,
                                 @SystemTablePlacement final String placement,
                                 DataStore dataStore) {
        _dataStore = dataStore;

        _tableNameSupplier = Suppliers.memoize(
            new Supplier<String>() {
                @Override
                public String get() {
                    // Lazily ensure the table exists on the first use.
                    String tableName = tableNameSupplier.get();
                    if (!_dataStore.getTableExists(tableName)) {
                        _dataStore.createTable(tableName,
                                new TableOptionsBuilder().setPlacement(placement).build(),
                                ImmutableMap.<String, String>of(),
                                new AuditBuilder().setLocalHost().setComment("create table").build()
                        );
                    }
                    return tableName;
                }
            }
        );
    }

    private String getTableName() {
        return _tableNameSupplier.get();
    }

    @Override
    public <Q, R> void updateJobStatus(JobIdentifier<Q, R> jobId, JobStatus<Q, R> jobStatus) {
        checkNotNull(jobId, "jobId");
        checkNotNull(jobStatus, "jobStatus");

        Delta delta = Deltas.mapBuilder()
                .put("status", JsonHelper.convert(jobStatus, Object.class))
                .build();

        write(jobId, delta, "Update job status to " + jobStatus.getStatus());
    }

    @Nullable
    @Override
    public <Q, R> JobStatus<Q, R> getJobStatus(JobIdentifier<Q, R> jobId) {
        checkNotNull(jobId, "jobId");

        Map<String, Object> result = _dataStore.get(getTableName(), jobId.toString(), ReadConsistency.STRONG);

        if (Intrinsic.isDeleted(result)) {
            return null;
        }

        Object status = result.get("status");
        return narrow(status, jobId.getJobType());
    }

    @Override
    public void deleteJobStatus(JobIdentifier<?, ?> jobId) {
        checkNotNull(jobId, "jobId");

        write(jobId, Deltas.delete(), "Deleting job status");
    }

    private void write(JobIdentifier<?, ?> jobId, Delta delta, String comment) {
        _dataStore.update(
                getTableName(),
                jobId.toString(),
                TimeUUIDs.newUUID(),
                delta,
                new AuditBuilder().setLocalHost().setComment(comment).build(),
                WriteConsistency.STRONG
        );
    }
}
