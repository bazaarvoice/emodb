package com.bazaarvoice.emodb.cachemgr.invalidate;

import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.net.URI;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Enumerates cache invalidation end points for foreign data centers based on a static list of end points.
 * <p>
 * Presumably, each of the end points point to a load balancer (Elastic IP, etc.) that will route to a
 * live server within the data center.
 */
public class ForeignDataCenterEndPointProvider implements EndPointProvider {
    private final InvalidationServiceEndPointAdapter _endPointAdapter;
    private final DataCenters _dataCenters;

    @Inject
    public ForeignDataCenterEndPointProvider(InvalidationServiceEndPointAdapter endPointAdapter,
                                             DataCenters dataCenters) {
        _endPointAdapter = checkNotNull(endPointAdapter, "endPointAdapter");
        _dataCenters = checkNotNull(dataCenters, "dataCenters");
    }

    @Override
    public void withEndPoints(Function<Collection<EndPoint>, ?> function) {
        List<EndPoint> endPoints = Lists.newArrayList();
        DataCenter self = _dataCenters.getSelf();
        for (DataCenter dataCenter : _dataCenters.getAll()) {
            if (!dataCenter.equals(self)) {
                final URI adminUri = dataCenter.getAdminUri();
                endPoints.add(new EndPoint() {
                    @Override
                    public String getAddress() {
                        return _endPointAdapter.toEndPointAddress(adminUri);
                    }

                    @Override
                    public boolean isValid() {
                        return true;
                    }
                });
            }
        }

        function.apply(endPoints);
    }
}
