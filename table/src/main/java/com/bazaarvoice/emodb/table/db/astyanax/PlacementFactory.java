package com.bazaarvoice.emodb.table.db.astyanax;

import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.sor.api.UnknownPlacementException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

import java.util.Collection;

public interface PlacementFactory {

    Collection<String> getValidPlacements();

    boolean isValidPlacement(String placement);

    boolean isAvailablePlacement(String placement);

    Placement newPlacement(String placement)
            throws UnknownPlacementException, ConnectionException;

    Collection<DataCenter> getDataCenters(String placement);
}
