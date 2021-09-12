package com.bazaarvoice.emodb.table.db.eventregistry;

import java.util.stream.Stream;

public interface TableEventTools {
    Stream<String> getIdsForStorage(String table, String uuid);
}
