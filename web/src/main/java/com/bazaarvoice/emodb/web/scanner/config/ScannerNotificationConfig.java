package com.bazaarvoice.emodb.web.scanner.config;

import com.bazaarvoice.emodb.plugin.PluginConfiguration;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

public class ScannerNotificationConfig {

    // Name of the SNS topic for posting scanner notifications; null disables SNS notifications
    @Valid
    @JsonProperty ("snsTopic")
    private String _snsTopic = null;

    // Enables posting active and pending scan counts as CloudWatch metrics
    @Valid
    @NotNull
    @JsonProperty ("enableCloudWatchMetrics")
    private boolean _enableCloudWatchMetrics = true;

    @Valid
    @NotNull
    @JsonProperty ("cloudWatchDimensions")
    private Map<String, String> _cloudWatchDimensions = ImmutableMap.of();

    @Valid
    @NotNull
    @JsonProperty ("stashStateListeners")
    private List<PluginConfiguration> _stashStateListenerPluginConfigurations = ImmutableList.of();

    public String getSnsTopic() {
        return _snsTopic;
    }

    public void setSnsTopic(String snsTopic) {
        _snsTopic = snsTopic;
    }

    public boolean isEnableCloudWatchMetrics() {
        return _enableCloudWatchMetrics;
    }

    public void setEnableCloudWatchMetrics(boolean enableCloudWatchMetrics) {
        _enableCloudWatchMetrics = enableCloudWatchMetrics;
    }

    public Map<String, String> getCloudWatchDimensions() {
        return _cloudWatchDimensions;
    }

    public void setCloudWatchDimensions(Map<String, String> cloudWatchDimensions) {
        _cloudWatchDimensions = cloudWatchDimensions;
    }

    public List<PluginConfiguration> getStashStateListenerPluginConfigurations() {
        return _stashStateListenerPluginConfigurations;
    }

    public void setStashStateListenerPluginConfigurations(List<PluginConfiguration> stashStateListenerPluginConfigurations) {
        _stashStateListenerPluginConfigurations = stashStateListenerPluginConfigurations;
    }
}
