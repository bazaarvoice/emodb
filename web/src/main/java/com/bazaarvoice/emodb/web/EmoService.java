package com.bazaarvoice.emodb.web;

import com.bazaarvoice.emodb.auth.dropwizard.DropwizardAuthConfigurator;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.cachemgr.invalidate.InvalidationService;
import com.bazaarvoice.emodb.common.dropwizard.discovery.ManagedRegistration;
import com.bazaarvoice.emodb.common.dropwizard.discovery.ResourceRegistry;
import com.bazaarvoice.emodb.common.dropwizard.jersey.ServerErrorResponseMetricsFilter;
import com.bazaarvoice.emodb.common.dropwizard.jersey.UnbufferedStreamFilter;
import com.bazaarvoice.emodb.common.dropwizard.jersey.UnbufferedStreamDynamicFeature;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.metrics.EmoGarbageCollectorMetricSet;
import com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode;
import com.bazaarvoice.emodb.common.zookeeper.store.MapStore;
import com.bazaarvoice.emodb.databus.core.DatabusEventStore;
import com.bazaarvoice.emodb.databus.repl.ReplicationSource;
import com.bazaarvoice.emodb.plugin.lifecycle.ServerStartedListener;
import com.bazaarvoice.emodb.queue.api.DedupQueueService;
import com.bazaarvoice.emodb.queue.api.QueueService;
import com.bazaarvoice.emodb.queue.client.DedupQueueServiceAuthenticator;
import com.bazaarvoice.emodb.queue.client.QueueServiceAuthenticator;
import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.compactioncontrol.LocalCompactionControl;
import com.bazaarvoice.emodb.sor.core.DataStoreAsync;
import com.bazaarvoice.emodb.web.auth.EncryptConfigurationApiKeyCommand;
import com.bazaarvoice.emodb.web.cli.AllTablesReportCommand;
import com.bazaarvoice.emodb.web.cli.ListCassandraCommand;
import com.bazaarvoice.emodb.web.cli.PurgeDatabusEventsCommand;
import com.bazaarvoice.emodb.web.cli.RegisterCassandraCommand;
import com.bazaarvoice.emodb.web.cli.UnregisterCassandraCommand;
import com.bazaarvoice.emodb.web.ddl.CreateKeyspacesCommand;
import com.bazaarvoice.emodb.web.ddl.DdlConfiguration;
import com.bazaarvoice.emodb.web.jersey.ExceptionMappers;
import com.bazaarvoice.emodb.web.megabus.resource.MegabusResource1;
import com.bazaarvoice.emodb.web.partition.PartitionAwareClient;
import com.bazaarvoice.emodb.web.report.ReportLoader;
import com.bazaarvoice.emodb.web.resources.FaviconResource;
import com.bazaarvoice.emodb.web.resources.blob.ApprovedBlobContentTypes;
import com.bazaarvoice.emodb.web.resources.blob.BlobStoreResource1;
import com.bazaarvoice.emodb.web.resources.databus.DatabusResource1;
import com.bazaarvoice.emodb.web.resources.databus.DatabusResourcePoller;
import com.bazaarvoice.emodb.web.resources.databus.ReplicationResource1;
import com.bazaarvoice.emodb.web.resources.databus.SubjectDatabus;
import com.bazaarvoice.emodb.web.resources.queue.DedupQueueResource1;
import com.bazaarvoice.emodb.web.resources.queue.QueueResource1;
import com.bazaarvoice.emodb.web.resources.report.ReportResource1;
import com.bazaarvoice.emodb.web.resources.sor.DataStoreResource1;
import com.bazaarvoice.emodb.web.resources.uac.ApiKeyResource1;
import com.bazaarvoice.emodb.web.resources.uac.RoleResource1;
import com.bazaarvoice.emodb.web.resources.uac.UserAccessControlRequestMessageBodyReader;
import com.bazaarvoice.emodb.web.resources.uac.UserAccessControlResource1;
import com.bazaarvoice.emodb.web.scanner.ScanUploader;
import com.bazaarvoice.emodb.web.scanner.resource.StashResource1;
import com.bazaarvoice.emodb.web.scanner.scheduling.StashRequestManager;
import com.bazaarvoice.emodb.web.throttling.AdHocConcurrentRequestRegulatorSupplier;
import com.bazaarvoice.emodb.web.throttling.AdHocThrottleManager;
import com.bazaarvoice.emodb.web.throttling.BlackListIpValueStore;
import com.bazaarvoice.emodb.web.throttling.BlackListedIpFilter;
import com.bazaarvoice.emodb.web.throttling.ConcurrentRequestsThrottlingFilter;
import com.bazaarvoice.emodb.web.throttling.DataStoreUpdateThrottler;
import com.bazaarvoice.emodb.web.throttling.ThrottlingFilterFactory;
import com.bazaarvoice.emodb.web.uac.SubjectUserAccessControl;
import com.bazaarvoice.emodb.web.util.EmoServiceObjectMapperFactory;
import com.bazaarvoice.megabus.MegabusSource;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.servlets.PingServlet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.io.Closeables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.dropwizard.Application;
import io.dropwizard.configuration.ConfigurationException;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.lifecycle.ServerLifecycleListener;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.swagger.jaxrs.config.BeanConfig;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.commons.lang.ArrayUtils;
import org.apache.curator.framework.CuratorFramework;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.FilterHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.DispatcherType;
import javax.validation.Validation;
import javax.validation.Validator;
import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.blackList;
import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.blobStore_web;
import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.cache;
import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.dataBus_web;
import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.dataStore_web;
import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.invalidation_cache_listener;
import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.megabus;
import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.queue_web;
import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.report;
import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.scanner;
import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.security;
import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.swagger;
import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.throttle;
import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.uac;
import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.web;
import static com.google.common.base.Preconditions.checkNotNull;

public class EmoService extends Application<EmoConfiguration> {

    private EmoConfiguration _configuration;
    private EmoServiceMode _serviceMode;
    private Environment _environment;
    private Injector _injector;
    private String _cluster;
    private final String _pathToDdl;
    private final File _configFile;

    private static final Logger _log = LoggerFactory.getLogger(EmoService.class);

    public static void main(String... args) throws Exception {

        ArgumentParser parser = ArgumentParsers.newArgumentParser("emoparser");
        parser.addArgument("server").required(true).help("server");
        parser.addArgument("emo-config").required(true).help("config.yaml");
        parser.addArgument("config-ddl").required(true).help("config-ddl.yaml");

        // Get the path to config-ddl
        String[] first3Args = (String[]) ArrayUtils.subarray(args, 0, Math.min(args.length, 3));
        Namespace result = parser.parseArgs(first3Args);

        // Remove config-ddl arg
        new EmoService(result.getString("config-ddl"), new File(result.getString("emo-config")))
                .run((String[]) ArrayUtils.remove(args, 2));
    }

    public EmoService(String emoDdlPath, File configFile) {
        _pathToDdl = checkNotNull(emoDdlPath, "emoDdlPath");
        _configFile = checkNotNull(configFile, "configFile");
    }

    @Override
    public void initialize(Bootstrap<EmoConfiguration> bootstrap) {
        bootstrap.addCommand(new CreateKeyspacesCommand());
        bootstrap.addCommand(new RegisterCassandraCommand());
        bootstrap.addCommand(new ListCassandraCommand());
        bootstrap.addCommand(new UnregisterCassandraCommand());
        bootstrap.addCommand(new PurgeDatabusEventsCommand());
        bootstrap.addCommand(new AllTablesReportCommand());
        bootstrap.addCommand(new EncryptConfigurationApiKeyCommand());
        EmoServiceObjectMapperFactory.configure(bootstrap.getObjectMapper());

        bootstrap.getMetricRegistry().register("jvm.gc.totals", new EmoGarbageCollectorMetricSet());
    }

    @Override
    public void run(EmoConfiguration configuration, Environment environment)
            throws Exception {

        _configuration = configuration;
        _environment = environment;
        _cluster = _configuration.getCluster();
        _serviceMode = _configuration.getServiceMode();
        _log.info("mode {}", _serviceMode);

        // Create cassandra schema before starting Emo. This is a no-op if schemas are already created.
        CuratorFramework curator = _configuration.getZooKeeperConfiguration().newCurator();
        try {
            CreateKeyspacesCommand createKeyspacesCommand = new CreateKeyspacesCommand();
            curator.start();
            DdlConfiguration ddlConfiguration = CreateKeyspacesCommand.parseDdlConfiguration(new File(_pathToDdl));
            // Note: Do not use the same EmoConfiguration passed as the method parameter.
            // Reusing it will result in IllegalState in CassandraConfiguration#withZooKeeperHostDiscovery
            EmoConfiguration emoConfiguration = loadConfigFile(_configFile);
            createKeyspacesCommand.createKeyspacesIfNecessary(emoConfiguration, ddlConfiguration, curator,
                    new MetricRegistry());
        } finally {
            Closeables.close(curator, true);
        }

        // Configure Jersey exception mappers
        for (Object mapper : ExceptionMappers.getMappers()) {
            environment.jersey().register(mapper);
        }
        for (Class mapperType : ExceptionMappers.getMapperTypes()) {
            environment.jersey().register(mapperType);
        }

        // Configure support for streaming JSON responses without long delays due to buffering
        //noinspection unchecked
        environment.jersey().getResourceConfig().register(new UnbufferedStreamDynamicFeature());
        environment.getApplicationContext().addFilter(new FilterHolder(new UnbufferedStreamFilter()), "/*", EnumSet.of(DispatcherType.REQUEST));

        // Create all the major EmoDB components using Guice.  Note: This code is organized such that almost all
        // initialization is complete before we register with Ostrich so we don't start receiving inbound requests
        // before the server is ready and listening.
        _injector = Guice.createInjector(new EmoModule(_configuration, _environment, _serviceMode));

        evaluateWeb();
        evaluateInvalidateCaches();
        evaluateDataStore();
        evaluateBlobStore();
        evaluateDatabus();
        evaluateQueue();
        evaluateBlackList();
        evaluateReporting();
        evaluateThrottling();
        evaluateSecurity();
        evaluateScanner();
        evaluateMegabus();
        evaluateServiceStartedListeners();
        evaluateSwagger();
        evaluateUAC();
    }

    private void evaluateWeb()
            throws Exception {
        if (!runPerServiceMode(web)) {
            return;
        }

        // Load balancers should hit the ping servlet, exposed on the main port to reflect main connection pool issues
        _environment.servlets().addServlet("/ping", new PingServlet());
        // Serve static assets
        _environment.jersey().register(FaviconResource.class);

        // Add a filter to provide finer 5xx metrics than the default DropWizard metrics include.
        //noinspection unchecked
        _environment.jersey().getResourceConfig().register(new ServerErrorResponseMetricsFilter(_environment.metrics()));
    }

    private void evaluateInvalidateCaches()
            throws Exception {
        if (runPerServiceMode(cache) && runPerServiceMode(invalidation_cache_listener)) {
            // Start handling cache flush requests
            _environment.lifecycle().manage(_injector.getInstance(Key.get(ManagedRegistration.class, InvalidationService.class)));
        }
    }

    private void evaluateDataStore()
            throws Exception {
        if (!runPerServiceMode(dataStore_web)) {
            return;
        }

        DataStore dataStore = _injector.getInstance(DataStore.class);
        ResourceRegistry resources = _injector.getInstance(ResourceRegistry.class);
        DataStoreAsync dataStoreAsync = _injector.getInstance(DataStoreAsync.class);
        CompactionControlSource compactionControlSource = _injector.getInstance(Key.get(CompactionControlSource.class, LocalCompactionControl.class));
        DataStoreUpdateThrottler updateThrottle = _injector.getInstance(DataStoreUpdateThrottler.class);
        
        // Start the System Of Record service
        resources.addResource(_cluster, "emodb-sor-1", new DataStoreResource1(dataStore, dataStoreAsync, compactionControlSource, updateThrottle));
    }

    private void evaluateBlobStore()
            throws Exception {
        if (!runPerServiceMode(blobStore_web)) {
            return;
        }

        BlobStore blobStore = _injector.getInstance(BlobStore.class);
        Set<String> approvedContentTypes = _injector.getInstance(
                Key.get(new TypeLiteral<Set<String>>(){}, ApprovedBlobContentTypes.class));
        // Start the Blob service
        ResourceRegistry resources = _injector.getInstance(ResourceRegistry.class);
        resources.addResource(_cluster, "emodb-blob-1", new BlobStoreResource1(blobStore, approvedContentTypes, _environment.metrics()));
    }

    private void evaluateDatabus()
            throws Exception {
        if (!runPerServiceMode(dataBus_web)) {
            return;
        }

        SubjectDatabus databus = _injector.getInstance(SubjectDatabus.class);
        SubjectDatabus databusClient = _injector.getInstance(Key.get(SubjectDatabus.class, PartitionAwareClient.class));
        DatabusEventStore databusEventStore = _injector.getInstance(DatabusEventStore.class);
        ReplicationSource replicationSource = _injector.getInstance(ReplicationSource.class);

        DatabusResourcePoller databusResourcePoller = _injector.getInstance(DatabusResourcePoller.class);

        // The Databus cross-data center replication end point is not discoverable via ZooKeeper, only ELB.

        _environment.jersey().register(new ReplicationResource1(replicationSource));

        // Start the Databus service
        ResourceRegistry resources = _injector.getInstance(ResourceRegistry.class);
        // Start the Databus service
        resources.addResource(_cluster, "emodb-bus-1",
                new DatabusResource1(databus, databusClient, databusEventStore, databusResourcePoller));
    }

    private void evaluateQueue()
            throws Exception {
        if (!runPerServiceMode(queue_web)) {
            return;
        }

        QueueService queueService = _injector.getInstance(QueueService.class);
        QueueServiceAuthenticator queueClient = _injector.getInstance(Key.get(QueueServiceAuthenticator.class, PartitionAwareClient.class));

        DedupQueueService dedupQueueService = _injector.getInstance(DedupQueueService.class);
        DedupQueueServiceAuthenticator dedupQueueClient = _injector.getInstance(Key.get(DedupQueueServiceAuthenticator.class, PartitionAwareClient.class));

        // Start the Queue service
        ResourceRegistry resources = _injector.getInstance(ResourceRegistry.class);
        // Start the Queue service
        resources.addResource(_cluster, "emodb-queue-1", new QueueResource1(queueService, queueClient));
        // Start the Dedup Queue service
        resources.addResource(_cluster, "emodb-dedupq-1", new DedupQueueResource1(dedupQueueService, dedupQueueClient));
    }

    private void evaluateScanner()
            throws Exception {
        if (!runPerServiceMode(scanner)) {
            return;
        }

        ResourceRegistry resources = _injector.getInstance(ResourceRegistry.class);
        ScanUploader scanUploader = _injector.getInstance(ScanUploader.class);
        StashRequestManager stashRequestManager = _injector.getInstance(StashRequestManager.class);

        resources.addResource(_cluster, "emodb-stash-1", new StashResource1(scanUploader, stashRequestManager));
        // No admin tasks are registered automatically in SCANNER ServiceMode
        _environment.admin().addTask(_injector.getInstance(LeaderServiceTask.class));
    }

    private void evaluateMegabus()
            throws Exception {
        if (!runPerServiceMode(megabus)) {
            return;
        }

        ResourceRegistry resources = _injector.getInstance(ResourceRegistry.class);
        MegabusSource megabusSource = _injector.getInstance(MegabusSource.class);
        resources.addResource(_cluster, "emodb-megabus-1", new MegabusResource1(megabusSource));
    }

    private void evaluateBlackList()
            throws Exception {
        if (!runPerServiceMode(blackList)) {
            return;
        }

        MapStore<Long> blackListIpValueStore =
                _injector.getInstance(Key.get(new TypeLiteral<MapStore<Long>>() {
                }, BlackListIpValueStore.class));

        // Allow manually specifying IPs to be throttled or blacklisted.
        _environment.getApplicationContext().addFilter(new FilterHolder(new BlackListedIpFilter(blackListIpValueStore)), "/*", EnumSet.of(DispatcherType.REQUEST));
    }

    private void evaluateReporting()
            throws Exception {
        if (!runPerServiceMode(report)) {
            return;
        }

        // Add the reporting endpoint.  This is for internal use and is therefore not discoverable.
        _environment.jersey().register(new ReportResource1(_injector.getInstance(ReportLoader.class)));
    }

    private void evaluateThrottling()
            throws Exception {
        if (!runPerServiceMode(throttle)) {
            return;
        }

        AdHocThrottleManager adHocThrottleManager = _injector.getInstance(AdHocThrottleManager.class);

        // Add filter to allow ad-hoc throttling of API calls
        ConcurrentRequestsThrottlingFilter adHocThrottleFilter =
                new ConcurrentRequestsThrottlingFilter(new AdHocConcurrentRequestRegulatorSupplier(adHocThrottleManager, _environment.metrics()));

        // Add a resource factory that creates throttling related Resource Filters for appropriate resource methods
        //noinspection unchecked
        _environment.jersey().getResourceConfig().register(new ThrottlingFilterFactory(_environment.metrics()));

        //noinspection unchecked
        _environment.jersey().getResourceConfig().register(adHocThrottleFilter);
    }

    private void evaluateSecurity() {
        if (!runPerServiceMode(security)) {
            return;
        }

        DropwizardAuthConfigurator authConfigurator = _injector.getInstance(DropwizardAuthConfigurator.class);
        // Add API Key authentication and authorization
        authConfigurator.configure(_environment);
    }

    private void evaluateServiceStartedListeners() throws Exception {
        final List<ServerStartedListener> listeners = _injector.getInstance(
                Key.get(new TypeLiteral<List<ServerStartedListener>>() {}));

        if (!listeners.isEmpty()) {
            _environment.lifecycle().addServerLifecycleListener(new ServerLifecycleListener() {
                @Override
                public void serverStarted(Server server) {
                    for (ServerStartedListener listener : listeners) {
                        listener.serverStarted();
                    }
                }
            });
        }
    }

    private void evaluateSwagger()
            throws Exception {
        if (!runPerServiceMode(swagger)) {
            return;
        }

        // Add swagger providers
        _environment.jersey().register(io.swagger.jaxrs.listing.ApiListingResource.class);
        _environment.jersey().register(io.swagger.jaxrs.listing.SwaggerSerializers.class);

        // Configure and initialize swagger
        BeanConfig beanConfig = new BeanConfig();
        beanConfig.setVersion("1.0");
        beanConfig.setTitle("EMO REST Resources");
        beanConfig.setSchemes(new String[] {"http"});
        beanConfig.setHost("localhost:8080");
        beanConfig.setBasePath("/");
        // add the packages that swagger should scan to pick up the resources
        beanConfig.setResourcePackage("com.bazaarvoice.emodb.web.resources");
        // this is a MUST and should be the last property - this creates a new SwaggerContextService and initialize the scanner.
        beanConfig.setScan(true);
    }

    private void evaluateUAC()
            throws Exception {
        if (!runPerServiceMode(uac)) {
            return;
        }

        _environment.jersey().register(UserAccessControlRequestMessageBodyReader.class);

        ResourceRegistry resources = _injector.getInstance(ResourceRegistry.class);
        SubjectUserAccessControl subjectUserAccessControl = _injector.getInstance(SubjectUserAccessControl.class);
        RoleResource1 roleResource1 = new RoleResource1(subjectUserAccessControl);
        ApiKeyResource1 apiKeyResource = new ApiKeyResource1(subjectUserAccessControl);
        UserAccessControlResource1 authResource = new UserAccessControlResource1(roleResource1, apiKeyResource);
        resources.addResource(_cluster, "emodb-uac-1", authResource);
    }

    private boolean runPerServiceMode (EmoServiceMode.Aspect aspect) {
        if (_serviceMode.specifies(aspect)) {
            _log.info("running {}", aspect);
            return true;
        }
        return false;
    }

    private EmoConfiguration loadConfigFile(File configFile)
            throws IOException, ConfigurationException {
        Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
        ObjectMapper mapper = EmoServiceObjectMapperFactory.build(new YAMLFactory());
        ConfigurationFactory<EmoConfiguration> configurationFactory = new ConfigurationFactory(EmoConfiguration.class, validator, mapper, "dw");
        return configurationFactory.build(configFile);
    }
}