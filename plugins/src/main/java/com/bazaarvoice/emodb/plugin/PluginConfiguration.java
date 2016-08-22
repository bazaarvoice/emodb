package com.bazaarvoice.emodb.plugin;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Map;

/**
 * Configuration object for an EmoDB plugin as intended to be used in the server's yaml configuration.  The
 * configuration has two components:
 *
 * <ol>
 *     <li>The fully-qualified plugin implementation class.</li>
 *     <li>If necessary, a yaml view of the implementation class' constructor configuration object.</li>
 * </ol>
 *
 * For example, the following plugin com.x.y.SamplePlugin, and its configuration object, com.x.y.SamplePluginConfig:
 *
 * <code>
 *     public class SamplePlugin implements ServerStartedListener<SamplePluginConfig> {
 *         public void init(Environment environment, PluginServerMetadata metadata, SamplePluginConfig config) {
 *             ...
 *         }
 *
 *         ...
 *     }
 *
 *     public class SamplePluginConfig() {
 *         @JsonProperty private String name;
 *         @JsonProperty private double rate;
 *     }
 * </code>
 *
 * The following yaml could be used to provide and instantiate this plugin:
 *
 * <code>
 *     serverStartedListeners:
 *     - class: com.x.y.SamplePlugin
 *       config:
 *         name: SampleName
 *         rate: 0.5
 * </code>
 */
public class PluginConfiguration {

    @Valid
    @NotNull
    @JsonProperty ("class")
    private String _className;

    @JsonProperty ("config")
    private Map<String, Object> _config = ImmutableMap.of();

    public String getClassName() {
        return _className;
    }

    public void setClassName(String className) {
        _className = className;
    }

    public Map<String, Object> getConfig() {
        return _config;
    }

    public void setConfig(Map<String, Object> config) {
        _config = config;
    }
}
