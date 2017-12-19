package com.bazaarvoice.emodb.web.scanner;

import com.bazaarvoice.emodb.common.dropwizard.guice.Global;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.guice.ServerCluster;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ManagedGuavaService;
import com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.plugin.PluginServerMetadata;
import com.bazaarvoice.emodb.sor.DataStoreConfiguration;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.core.DataTools;
import com.bazaarvoice.emodb.web.EmoConfiguration;
import com.bazaarvoice.emodb.web.scanner.config.ScannerConfiguration;
import com.beust.jcommander.internal.Lists;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import com.google.common.net.HostAndPort;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Module;
import com.sun.jersey.api.client.Client;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.time.Clock;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ScanUploadModuleTest {

    @Test
    public void testStashFullMode() {
        List<Object> managedInstances = createInjectorAndGetManagedInstances(EmoServiceMode.STASH);
        Set<String> managedServices = managedInstances.stream().map(this::toServiceName).collect(Collectors.toSet());
        assertTrue(managedServices.contains("scan-upload-scheduler"));
        assertTrue(managedServices.contains("scan-upload-monitor"));
        assertTrue(managedServices.contains("CloudWatchScanCountListener"));
        assertTrue(managedServices.contains("LocalRangeScanUploader"));
        assertTrue(managedServices.contains("DistributedScanRangeMonitor"));
        assertTrue(managedServices.contains("ScanParticipationService"));
    }

    @Test
    public void testStashManagerMode() {
        List<Object> managedInstances = createInjectorAndGetManagedInstances(EmoServiceMode.STASH_MANAGER);
        Set<String> managedServices = managedInstances.stream().map(this::toServiceName).collect(Collectors.toSet());
        assertTrue(managedServices.contains("scan-upload-scheduler"));
        assertTrue(managedServices.contains("scan-upload-monitor"));
        assertTrue(managedServices.contains("CloudWatchScanCountListener"));
        assertFalse(managedServices.contains("LocalRangeScanUploader"));
        assertFalse(managedServices.contains("DistributedScanRangeMonitor"));
        assertFalse(managedServices.contains("ScanParticipationService"));
    }

    @Test
    public void testStashWorkerMode() {
        List<Object> managedInstances = createInjectorAndGetManagedInstances(EmoServiceMode.STASH_WORKER);
        Set<String> managedServices = managedInstances.stream().map(this::toServiceName).collect(Collectors.toSet());
        assertFalse(managedServices.contains("scan-upload-scheduler"));
        assertFalse(managedServices.contains("scan-upload-monitor"));
        assertFalse(managedServices.contains("CloudWatchScanCountListener"));
        assertTrue(managedServices.contains("LocalRangeScanUploader"));
        assertTrue(managedServices.contains("DistributedScanRangeMonitor"));
        assertTrue(managedServices.contains("ScanParticipationService"));
    }

    private List<Object> createInjectorAndGetManagedInstances(EmoServiceMode serviceMode) {
        EmoConfiguration emoConfig = new EmoConfiguration();
        emoConfig.setServiceMode(serviceMode);
        emoConfig.setCluster("test_cluster");

        DataStoreConfiguration dataStoreConfig = new DataStoreConfiguration();
        dataStoreConfig.setSystemTablePlacement("app_global:sys");
        emoConfig.setDataStoreConfiguration(dataStoreConfig);

        ScannerConfiguration scannerConfig = new ScannerConfiguration();
        scannerConfig.setUseSQSQueues(false);
        scannerConfig.setAwsRegion(Optional.of("us-east-1"));
        emoConfig.setScanner(Optional.fromNullable(scannerConfig));

        final List<Object> managedInstances = Lists.newArrayList();

        Module dependenciesModules = new AbstractModule() {
            @Override
            protected void configure() {
                binder().requireExplicitBindings();

                // Bind a lifecycle registry which captures every instance of a managed object
                bind(LifeCycleRegistry.class).toInstance(new LifeCycleRegistry() {
                    @Override
                    public <T extends Managed> T manage(T managed) {
                        managedInstances.add(managed);
                        return managed;
                    }

                    @Override
                    public <T extends Closeable> T manage(T closeable) {
                        managedInstances.add(closeable);
                        return closeable;
                    }
                });

                // For the remaining dependencies mock out the bare minimum so all Stash module dependencies are satisfied
                Environment environment = mock(Environment.class);
                LifecycleEnvironment lifecycleEnvironment = mock(LifecycleEnvironment.class);
                doAnswer(invocation -> managedInstances.add(invocation.getArguments()[0])).when(lifecycleEnvironment).manage(any(Managed.class));
                when(environment.lifecycle()).thenReturn(lifecycleEnvironment);
                MetricRegistry metricRegistry = new MetricRegistry();
                when(environment.metrics()).thenReturn(metricRegistry);
                bind(Environment.class).toInstance(environment);
                bind(MetricRegistry.class).toInstance(metricRegistry);
                bind(HostAndPort.class).annotatedWith(SelfHostAndPort.class).toInstance(HostAndPort.fromParts("localhost", 8080));
                bind(Clock.class).toInstance(Clock.systemUTC());
                bind(TaskRegistry.class).toInstance(mock(TaskRegistry.class));
                bind(LeaderServiceTask.class).asEagerSingleton();
                bind(PluginServerMetadata.class).toInstance(mock(PluginServerMetadata.class));
                bind(DataStoreConfiguration.class).toInstance(emoConfig.getDataStoreConfiguration());
                bind(String.class).annotatedWith(ServerCluster.class).toInstance(emoConfig.getCluster());
                bind(Client.class).toInstance(mock(Client.class));
                bind(DataTools.class).toInstance(mock(DataTools.class));
                bind(DataStore.class).toInstance(mock(DataStore.class));

                CuratorFramework curator = mock(CuratorFramework.class);
                when(curator.getState()).thenReturn(CuratorFrameworkState.STARTED);
                bind(CuratorFramework.class).annotatedWith(Global.class).toInstance(curator);
                bind(CuratorFramework.class).annotatedWith(ScannerZooKeeper.class).toInstance(curator);

            }
        };

        ScanUploadModule module = new ScanUploadModule(serviceMode, emoConfig);

        Guice.createInjector(dependenciesModules, module);

        return managedInstances;
    }

    private String toServiceName(Object obj) {
        if (obj instanceof ManagedGuavaService) {
            // Extract the service name from the debug string
            Matcher matcher = Pattern.compile("ManagedGuavaService\\{(.+) \\[\\w+\\]\\}").matcher(obj.toString());
            assertTrue(matcher.matches());
            return matcher.group(1);
        }
        return obj.getClass().getSimpleName();
    }
}
