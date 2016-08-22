package com.bazaarvoice.emodb.table.db;

import com.google.common.base.Optional;

import java.util.Collection;

public interface Placements {

    Collection<String> getValidPlacements();

    /** Returns the name of the database/cassandra cluster in the current region that hosts this placement, if any. */
    Optional<String> getLocalCluster(String placement);
}
