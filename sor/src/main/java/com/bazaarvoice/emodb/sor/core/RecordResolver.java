package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.db.Record;

import java.util.Map;

public interface RecordResolver {

    String LAST_COMPACTED_MUTATION_KEY = "~lastCompactedMutation";
    String LAST_COMPACTION_CUTOFF_KEY = "~lastCompactionCutoff";

    Resolved resolve(Record record, ReadConsistency consistency);

    Map<String, Object> toContent(Resolved resolved, ReadConsistency consistency, boolean includeCompactionData);

}
