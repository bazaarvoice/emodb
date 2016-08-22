package com.bazaarvoice.emodb.web.plugins;

import com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode;
import com.bazaarvoice.emodb.plugin.PluginServerMetadata;
import com.google.common.net.HostAndPort;
import org.apache.curator.framework.CuratorFramework;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Simple POJO implementation of {@link PluginServerMetadata}.
 */
public class DefaultPluginServerMetadata implements PluginServerMetadata {

    private final EmoServiceMode _serviceMode;
    private final String _cluster;
    private final HostAndPort _serviceHostAndPort;
    private final HostAndPort _adminHostAndPort;
    private final String _version;
    private final CuratorFramework _curator;

    public DefaultPluginServerMetadata(EmoServiceMode serviceMode, String cluster, HostAndPort serviceHostAndPort, HostAndPort adminHostAndPort,
                                       String version, CuratorFramework curator) {
        _serviceMode = checkNotNull(serviceMode, "serviceMode");
        _cluster = checkNotNull(cluster, "cluster");
        _serviceHostAndPort = checkNotNull(serviceHostAndPort, "serviceHostAndPort");
        _adminHostAndPort = checkNotNull(adminHostAndPort, "adminHostAndPort");
        _version = checkNotNull(version, "version");
        _curator = checkNotNull(curator, "curator");
    }

    @Override
    public EmoServiceMode getServiceMode() {
        return _serviceMode;
    }

    @Override
    public String getCluster() {
        return _cluster;
    }

    @Override
    public HostAndPort getServiceHostAndPort() {
        return _serviceHostAndPort;
    }

    @Override
    public HostAndPort getAdminHostAndPort() {
        return _adminHostAndPort;
    }

    @Override
    public String getApplicationVersion() {
        return _version;
    }

    @Override
    public CuratorFramework getCurator() {
        return _curator;
    }
}
