package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.table.db.Table;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

/**
 * The minimal interface necessary to support the Databus, in concert with {@link UpdateRef}.
 */
public interface DataProvider {
    /** Returns the template associated with a table. */
    Table getTable(String table);

    /** Begins the process of retrieving a batch of content items from the data store. */
    AnnotatedGet prepareGetAnnotated(ReadConsistency consistency);

    interface AnnotatedGet {
        AnnotatedGet add(String table, String key) throws UnknownTableException;

        Iterator<AnnotatedContent> execute();
    }

    interface AnnotatedContent {
        /**
         * Returns the JSON entity object including {@link Intrinsic} fields.
         */
        Map<String, Object> getContent();

        /**
         * Is the specified change unknown such that, due to eventual consistency and/or replication lag the change
         * ID will become known in the near future?
         */
        boolean isChangeDeltaPending(UUID changeId);

        /**
         * Can it be proven that the change has no effect on the actual content (not including intrinsics)?  If so,
         * callers can usually ignore the change.
         */
        boolean isChangeDeltaRedundant(UUID changeId);
    }
}
