package com.bazaarvoice.emodb.web;

import com.bazaarvoice.curator.dropwizard.ZooKeeperConfiguration;
import com.bazaarvoice.emodb.blob.BlobStoreConfiguration;
import com.bazaarvoice.emodb.common.cassandra.CqlDriverConfiguration;
import com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode;
import com.bazaarvoice.emodb.databus.DatabusConfiguration;
import com.bazaarvoice.emodb.datacenter.DataCenterConfiguration;
import com.bazaarvoice.emodb.job.JobConfiguration;
import com.bazaarvoice.emodb.plugin.PluginConfiguration;
import com.bazaarvoice.emodb.queue.QueueConfiguration;
import com.bazaarvoice.emodb.sor.DataStoreConfiguration;
import com.bazaarvoice.emodb.web.auth.AuthorizationConfiguration;
import com.bazaarvoice.emodb.web.migrator.config.MigratorConfiguration;
import com.bazaarvoice.emodb.web.scanner.config.ScannerConfiguration;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.dropwizard.Configuration;
import io.dropwizard.client.JerseyClientConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;

public class EmoConfiguration extends Configuration {

    @JsonProperty("serviceMode")
    private EmoServiceMode _serviceMode;

    @Valid
    @NotNull
    @JsonProperty("cluster")
    private String _cluster;

    @Valid
    @NotNull
    @JsonProperty("systemOfRecord")
    private DataStoreConfiguration _dataStoreConfiguration;

    @Valid
    @NotNull
    @JsonProperty("databus")
    private DatabusConfiguration _databusConfiguration;

    @Valid
    @NotNull
    @JsonProperty("blobStore")
    private BlobStoreConfiguration _blobStoreConfiguration;

    @Valid
    @NotNull
    @JsonProperty("queueService")
    private QueueConfiguration _queueConfiguration;

    @Valid
    @NotNull
    @JsonProperty("dataCenter")
    private DataCenterConfiguration _dataCenterConfiguration = new DataCenterConfiguration();

    @Valid
    @NotNull
    @JsonProperty("cqlDriver")
    private CqlDriverConfiguration _cqlDriverConfiguration = new CqlDriverConfiguration();

    @Valid
    @NotNull
    @JsonProperty("jobs")
    private JobConfiguration _jobConfiguration = new JobConfiguration();

    @Valid
    @NotNull
    @JsonProperty("auth")
    private AuthorizationConfiguration _authorizationConfiguration = new AuthorizationConfiguration();

    @Valid
    @NotNull
    @JsonProperty("zooKeeper")
    private ZooKeeperConfiguration _zooKeeperConfiguration = new ZooKeeperConfiguration();

    @Valid
    @NotNull
    @JsonProperty("httpClient")
    private JerseyClientConfiguration _httpClientConfiguration = new JerseyClientConfiguration();

    @Valid
    @NotNull
    @JsonProperty ("scanner")
    private Optional<ScannerConfiguration> _scanner = Optional.absent();

    @Valid
    @NotNull
    @JsonProperty ("deltaMigrator")
    private Optional<MigratorConfiguration> _migrator = Optional.absent();

    @Valid
    @NotNull
    @JsonProperty ("serverStartedListeners")
    private List<PluginConfiguration> _serverStartedListenerPluginConfigurations = ImmutableList.of();

    public EmoServiceMode getServiceMode() {
        // Default mode if service mode not specified
        return _serviceMode == null ? EmoServiceMode.STANDARD_ALL : _serviceMode;
    }

    public EmoConfiguration setServiceMode(EmoServiceMode serviceMode) {
        _serviceMode = serviceMode;
        return this;
    }

    public String getCluster() {
        return _cluster;
    }

    public EmoConfiguration setCluster(String cluster) {
        _cluster = cluster;
        return this;
    }

    public DataStoreConfiguration getDataStoreConfiguration() {
        return _dataStoreConfiguration;
    }

    public EmoConfiguration setDataStoreConfiguration(DataStoreConfiguration dataStoreConfiguration) {
        _dataStoreConfiguration = dataStoreConfiguration;
        return this;
    }

    public DatabusConfiguration getDatabusConfiguration() {
        return _databusConfiguration;
    }

    public EmoConfiguration setDatabusConfiguration(DatabusConfiguration databusConfiguration) {
        _databusConfiguration = databusConfiguration;
        return this;
    }

    public BlobStoreConfiguration getBlobStoreConfiguration() {
        return _blobStoreConfiguration;
    }

    public EmoConfiguration setBlobStoreConfiguration(BlobStoreConfiguration blobStoreConfiguration) {
        _blobStoreConfiguration = blobStoreConfiguration;
        return this;
    }

    public QueueConfiguration getQueueConfiguration() {
        return _queueConfiguration;
    }

    public EmoConfiguration setQueueConfiguration(QueueConfiguration queueConfiguration) {
        _queueConfiguration = queueConfiguration;
        return this;
    }

    public DataCenterConfiguration getDataCenterConfiguration() {
        return _dataCenterConfiguration;
    }

    public EmoConfiguration setDataCenterConfiguration(DataCenterConfiguration dataCenterConfiguration) {
        _dataCenterConfiguration = dataCenterConfiguration;
        return this;
    }

    public CqlDriverConfiguration getCqlDriverConfiguration() {
        return _cqlDriverConfiguration;
    }

    public EmoConfiguration setCqlDriverConfiguration(CqlDriverConfiguration cqlDriverConfiguration) {
        _cqlDriverConfiguration = cqlDriverConfiguration;
        return this;
    }

    public JobConfiguration getJobConfiguration() {
        return _jobConfiguration;
    }

    public EmoConfiguration setJobConfiguration(JobConfiguration jobConfiguration) {
        _jobConfiguration = jobConfiguration;
        return this;
    }

    public AuthorizationConfiguration getAuthorizationConfiguration() {
        return _authorizationConfiguration;
    }

    public EmoConfiguration setAuthorizationConfiguration(AuthorizationConfiguration authorizationConfiguration) {
        _authorizationConfiguration = authorizationConfiguration;
        return this;
    }

    public ZooKeeperConfiguration getZooKeeperConfiguration() {
        return _zooKeeperConfiguration;
    }

    public EmoConfiguration setZooKeeperConfiguration(ZooKeeperConfiguration zooKeeperConfiguration) {
        _zooKeeperConfiguration = zooKeeperConfiguration;
        return this;
    }

    public JerseyClientConfiguration getHttpClientConfiguration() {
        return _httpClientConfiguration;
    }

    public EmoConfiguration setHttpClientConfiguration(JerseyClientConfiguration httpClientConfiguration) {
        _httpClientConfiguration = httpClientConfiguration;
        return this;
    }

    public Optional<ScannerConfiguration> getScanner() {
        return _scanner;
    }

    public Optional<MigratorConfiguration> getMigrator() {
        return _migrator;
    }

    public EmoConfiguration setScanner(Optional<ScannerConfiguration> scanner) {
        _scanner = scanner;
        return this;
    }

    public EmoConfiguration setMigrator(Optional<MigratorConfiguration> migrator) {
        _migrator = migrator;
        return this;
    }

    public List<PluginConfiguration> getServerStartedListenerPluginConfigurations() {
        return _serverStartedListenerPluginConfigurations;
    }

    public EmoConfiguration setServerStartedListenerPluginConfigurations(List<PluginConfiguration> serverStartedListenerPluginConfigurations) {
        _serverStartedListenerPluginConfigurations = serverStartedListenerPluginConfigurations;
        return this;
    }
}
