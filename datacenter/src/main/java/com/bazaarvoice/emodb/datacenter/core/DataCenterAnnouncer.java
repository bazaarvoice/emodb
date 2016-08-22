package com.bazaarvoice.emodb.datacenter.core;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.emodb.datacenter.api.KeyspaceDiscovery;
import com.bazaarvoice.emodb.datacenter.db.DataCenterDAO;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Updates the record for *this* data center in the System of Record so other clusters can discover it.
 */
public class DataCenterAnnouncer implements Managed {
    private static final Logger _log = LoggerFactory.getLogger(DataCenterAnnouncer.class);

    private final DataCenterDAO _dataCenterDao;
    private final List<KeyspaceDiscovery> _keyspaceDiscoveries;
    private final String _selfDataCenter;
    private final String _selfCassandraDataCenter;
    private final URI _selfServiceUri;
    private final URI _selfAdminUri;
    private final String _systemDataCenter;
    private final DataCenters _dataCenters;

    @Inject
    public DataCenterAnnouncer(LifeCycleRegistry lifeCycle, DataCenterDAO dataCenterDao,
                               @Named ("sor") KeyspaceDiscovery sorKeyspaceDiscovery,
                               @Named("blob") KeyspaceDiscovery blobKeyspaceDiscovery,
                               @SelfDataCenter String selfDataCenter,
                               @SelfCassandraDataCenter String selfCassandraDataCenter,
                               @SelfDataCenter URI selfServiceUri,
                               @SelfDataCenterAdmin URI selfAdminUri,
                               @SystemDataCenter String systemDataCenter,
                               DataCenters dataCenters) {
        _dataCenterDao = checkNotNull(dataCenterDao, "dataCenterDao");
        _keyspaceDiscoveries = ImmutableList.of(sorKeyspaceDiscovery, blobKeyspaceDiscovery);
        _selfDataCenter = checkNotNull(selfDataCenter, "selfDataCenter");
        _selfCassandraDataCenter = checkNotNull(selfCassandraDataCenter, "selfCassandraDataCenter");
        _selfServiceUri = checkNotNull(selfServiceUri, "selfServiceUri");
        _selfAdminUri = checkNotNull(selfAdminUri, "selfAdminUri");
        _systemDataCenter = checkNotNull(systemDataCenter, "systemDataCenter");
        _dataCenters = checkNotNull(dataCenters, "dataCenters");
        lifeCycle.manage(this);
    }

    @Override
    public void start() throws Exception {
        Set<String> cassandraKeyspaces = Sets.newTreeSet();
        for (KeyspaceDiscovery keyspaceDiscovery : _keyspaceDiscoveries) {
            cassandraKeyspaces.addAll(keyspaceDiscovery.getKeyspacesForDataCenter(_selfCassandraDataCenter));
        }
        boolean system = _selfDataCenter.equals(_systemDataCenter);
        DataCenter self = new DefaultDataCenter(_selfDataCenter, _selfServiceUri, _selfAdminUri, system,
                _selfCassandraDataCenter, cassandraKeyspaces);
        DataCenter original;
        try {
            original = _dataCenters.getSelf();
        } catch (Exception e) {
            original = null;  // self hasn't been announced yet.
        }
        if (_dataCenterDao.saveIfChanged(self, original)) {
            _log.info("Announced new data center: {}", self);
            _dataCenters.refresh();
        }
    }

    @Override
    public void stop() throws Exception {
        // Do nothing
    }
}
