package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.netflix.astyanax.model.ConsistencyLevel;

/**
 * Converts from DataStore {@link ReadConsistency} and {@link WriteConsistency} levels to
 * the Astyanax-specific {@link ConsistencyLevel} enum.
 */
abstract class SorConsistencies {

    public static ConsistencyLevel toAstyanax(ReadConsistency consistency) {
        switch (consistency) {
            case WEAK:
                return ConsistencyLevel.CL_LOCAL_ONE;           // first node to respond
            case STRONG:
                return ConsistencyLevel.CL_LOCAL_QUORUM;  // single data center quorum
            default:
                throw new UnsupportedOperationException(String.valueOf(consistency));
        }
    }

    public static com.datastax.driver.core.ConsistencyLevel toCql(ReadConsistency consistency) {
        switch (consistency) {
            case WEAK:
                return com.datastax.driver.core.ConsistencyLevel.LOCAL_ONE;
            case STRONG:
                return com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM;
            default:
                throw new UnsupportedOperationException((String.valueOf(consistency)));
        }
    }

    public static ConsistencyLevel toAstyanax(WriteConsistency consistency) {
        switch (consistency) {
            case NON_DURABLE:
                return ConsistencyLevel.CL_ANY;  // at least 1 node, allow hinted handoff
            case WEAK:
                return ConsistencyLevel.CL_TWO;  // usually at least 2 nodes, survives the permanent failure of any single node
            case STRONG:
                return ConsistencyLevel.CL_LOCAL_QUORUM;   // single data center quorum
            case GLOBAL:
                return ConsistencyLevel.CL_EACH_QUORUM;    // all data center quorum
            default:
                throw new UnsupportedOperationException(String.valueOf(consistency));
        }
    }

    public static com.datastax.driver.core.ConsistencyLevel toCql(WriteConsistency consistency) {
        switch (consistency) {
            case NON_DURABLE:
                return com.datastax.driver.core.ConsistencyLevel.ANY;  // at least 1 node, allow hinted handoff
            case WEAK:
                return com.datastax.driver.core.ConsistencyLevel.TWO;  // usually at least 2 nodes, survives the permanent failure of any single node
            case STRONG:
                return com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM;   // single data center quorum
            case GLOBAL:
                return com.datastax.driver.core.ConsistencyLevel.EACH_QUORUM;    // all data center quorum
            default:
                throw new UnsupportedOperationException(String.valueOf(consistency));
        }
    }
}
