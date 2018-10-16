package com.bazaarvoice.emodb.table.db.astyanax;

import com.bazaarvoice.emodb.common.dropwizard.guice.SystemTablePlacement;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.google.inject.Inject;

/**
 * A near identical implementation of {@link CQLStashTableDAO} used by the migrator. The only difference between the two
 * is that this version uses a different Cassandra table and disables TTL.
 */
public class CqlMigratorTableDAO extends CQLStashTableDAO {

    @Inject
    public CqlMigratorTableDAO(@SystemTablePlacement String systemTablePlacement,
                            PlacementCache placementCache, DataCenters dataCenters) {
        super(systemTablePlacement, placementCache, dataCenters);
        STASH_TOKEN_RANGE_TABLE = "migrator_token_range";
        TTL = 0;
    }
}
