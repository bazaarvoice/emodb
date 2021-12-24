package com.bazaarvoice.emodb.table.db;

import java.util.Collection;
import java.util.Optional;

public interface Placements {

    Collection<String> getValidPlacements();

    /** Returns the name of the database/cassandra cluster in the current region that hosts this placement, if any. */
    Optional<String> getLocalCluster(String placement);
}
