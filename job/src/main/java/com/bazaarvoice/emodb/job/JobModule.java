package com.bazaarvoice.emodb.job;

import com.bazaarvoice.emodb.common.dropwizard.guice.SystemTablePlacement;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.emodb.job.admin.ControlJobServiceTask;
import com.bazaarvoice.emodb.job.api.JobHandlerRegistry;
import com.bazaarvoice.emodb.job.api.JobService;
import com.bazaarvoice.emodb.job.dao.DataStoreJobStatusDAO;
import com.bazaarvoice.emodb.job.dao.JobStatusDAO;
import com.bazaarvoice.emodb.job.dao.JobsTableName;
import com.bazaarvoice.emodb.job.handler.DefaultJobHandlerRegistry;
import com.bazaarvoice.emodb.job.handler.JobHandlerRegistryInternal;
import com.bazaarvoice.emodb.job.service.DefaultJobService;
import com.bazaarvoice.emodb.job.service.JobConcurrencyLevel;
import com.bazaarvoice.emodb.job.service.JobQueueName;
import com.bazaarvoice.emodb.job.service.NotOwnerRetryDelay;
import com.bazaarvoice.emodb.job.service.QueuePeekLimit;
import com.bazaarvoice.emodb.job.service.QueueRefreshTime;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.google.common.base.Supplier;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.curator.framework.CuratorFramework;
import org.joda.time.Duration;

import static java.lang.String.format;

/**
 * Guice module for constructing a {@link JobService}.
 * <p>
 * Requires the following external references:
 * <ul>
 * <li> {@link JobConfiguration}
 * <li> {@link DataStore}
 * <li> {@link DataCenters}
 * <li> {@link LifeCycleRegistry}
 * <li> {@link TaskRegistry}
 * <li> @{@link JobZooKeeper} {@link CuratorFramework}
 * </ul>
 * Exports the following:
 * <ul>
 * <li> {@link JobService}
 * <li> {@link JobHandlerRegistry}
 * </ul>
 */
public class JobModule extends PrivateModule {

    private final static String JOBS_TABLE_NAME_FORMAT = "__system_job:status-%s";
    private final static String QUEUE_NAME = "emodb:job";

    private final EmoServiceMode _serviceMode;

    public JobModule(EmoServiceMode serviceMode) {
        _serviceMode = serviceMode;
    }

    @Override
    protected void configure() {

        requireBinding(Key.get(String.class, SystemTablePlacement.class));

        bind(String.class).annotatedWith(JobQueueName.class).toInstance(QUEUE_NAME);

        bind(JobStatusDAO.class).to(DataStoreJobStatusDAO.class).asEagerSingleton();
        bind(JobHandlerRegistryInternal.class).to(DefaultJobHandlerRegistry.class).asEagerSingleton();
        bind(JobService.class).to(DefaultJobService.class).asEagerSingleton();
        bind(ControlJobServiceTask.class).asEagerSingleton();

        expose(JobService.class);
        expose(JobHandlerRegistry.class);
    }

    @Provides @Singleton @JobsTableName
    protected Supplier<String> provideQueueName(final DataCenters dataCenters) {
        return new Supplier<String>() {
            @Override
            public String get() {
                return format(JOBS_TABLE_NAME_FORMAT, dataCenters.getSelf().getName().replaceAll("\\s", "_"));
            }
        };
    }

    @Provides @Singleton @JobConcurrencyLevel
    protected Integer provideJobConcurrencyLevel(JobConfiguration configuration) {
        if (_serviceMode.specifies(EmoServiceMode.Aspect.job)) {
            return configuration.getConcurrencyLevel();
        }
        // Don't process jobs if not running in server mode
        return 0;
    }

    @Provides @Singleton @QueueRefreshTime
    protected Duration provideQueueRefreshTime(JobConfiguration configuration) {
        return configuration.getQueueRefreshTime();
    }

    @Provides @Singleton @QueuePeekLimit
    protected Integer provideQueuePeekLimit(JobConfiguration configuration) {
        return configuration.getQueuePeekLimit();
    }

    @Provides @Singleton @NotOwnerRetryDelay
    protected Duration provideNotOwnerRetryDelay(JobConfiguration configuration) {
        return configuration.getNotOwnerRetryDelay();
    }

    @Provides @Singleton
    public JobHandlerRegistry provideJobHandlerRegistry(JobHandlerRegistryInternal jobHandlerRegistryInternal) {
        return jobHandlerRegistryInternal;
    }
}
