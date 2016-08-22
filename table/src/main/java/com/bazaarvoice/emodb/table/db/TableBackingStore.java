package com.bazaarvoice.emodb.table.db;

import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.TableExistsException;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.delta.Delta;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

/**
 * Internal interface that may be used by table implementations to store table metadata.
 */
public interface TableBackingStore {

    void createTable(String table, TableOptions options, Map<String, ?> template, Audit audit)
            throws TableExistsException;

    void update(String table, String key, UUID changeId, Delta delta, Audit audit, WriteConsistency consistency);

    Map<String, Object> get(String table, String key, ReadConsistency consistency);

    Iterator<Map<String, Object>> scan(String table, @Nullable String fromKeyExclusive, LimitCounter limit, ReadConsistency consistency);
}
