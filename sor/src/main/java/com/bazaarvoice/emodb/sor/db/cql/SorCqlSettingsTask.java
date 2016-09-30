package com.bazaarvoice.emodb.sor.db.cql;

import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.sor.db.DataReaderDAO;
import com.bazaarvoice.emodb.sor.db.astyanax.CqlDataReaderDAO;
import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import io.dropwizard.servlets.tasks.Task;

import java.io.PrintWriter;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Update CQL driver settings.  Current configurable settings are:
 * Fetch size, batch fetch size, prefetch limit, batch prefetch limit
 *
 * <pre>
 *   curl -s -XPOST "http://localhost:8081/tasks/sor-cql-settings?fetchSize=15&prefetchLimit=5"
 * </pre>
 * To view the state of DataReaderDao:
 * <pre>
 *   curl -s -XPOST "http://localhost:8081/tasks/sor-cql-settings"
 * </pre>
 */
public class SorCqlSettingsTask extends Task {

    private final CqlDataReaderDAO _cqlDataReaderDAO;
    private final Supplier<Boolean> _useCqlForMultiGets;
    private final Supplier<Boolean> _useCqlForScans;

    @Inject
    public SorCqlSettingsTask(TaskRegistry taskRegistry, DataReaderDAO cqlDataReaderDAO,
                              @CqlForMultiGets Supplier<Boolean> useCqlForMultiGets,
                              @CqlForScans Supplier<Boolean> useCqlForScans) {
        super("sor-cql-settings");
        checkArgument(cqlDataReaderDAO instanceof CqlDataReaderDAO, "We should be using CQL now");
        //noinspection ConstantConditions
        _cqlDataReaderDAO = (CqlDataReaderDAO) cqlDataReaderDAO;
        _useCqlForMultiGets = useCqlForMultiGets;
        _useCqlForScans = useCqlForScans;
        taskRegistry.addTask(this);
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter out) throws Exception {
        String fetchValue = Iterables.getFirst(parameters.get("fetchSize"), "-1");
        String batchFetchValue = Iterables.getFirst(parameters.get("batchFetchSize"), "-1");
        String prefetchLimitValue = Iterables.getFirst(parameters.get("prefetchLimit"), "-1");
        String batchPrefetchLimitValue = Iterables.getFirst(parameters.get("batchPrefetchLimit"), "-1");
        Integer fetchSize = parseInt(fetchValue, "fetch size", out);
        Integer batchFetchSize = parseInt(batchFetchValue, "batch fetch size", out);
        Integer prefetchLimit = parseInt(prefetchLimitValue, "prefetch limit", out);
        Integer batchPrefetchLimit = parseInt(batchPrefetchLimitValue, "batch prefetch limit", out);

        if (fetchSize == null || batchFetchSize == null || prefetchLimit == null || batchPrefetchLimit == null) {
            return;
        }

        // TODO: At some point these should be revised to use the newer "settings" capabilities

        // Update fetch sizes and prefetch limits if needed
        if (fetchSize > 0 || prefetchLimit >= 0) {
            if (fetchSize <= 0) {
                fetchSize = _cqlDataReaderDAO.getSingleRowFetchSize();
            }
            if (prefetchLimit < 0) {
                prefetchLimit = _cqlDataReaderDAO.getSingleRowPrefetchLimit();
            }
            _cqlDataReaderDAO.setSingleRowFetchSizeAndPrefetchLimit(fetchSize, prefetchLimit);
        }
        if (batchFetchSize > 0 || batchPrefetchLimit >= 0) {
            if (batchFetchSize <= 0) {
                batchFetchSize = _cqlDataReaderDAO.getMultiRowFetchSize();
            }
            if (batchPrefetchLimit < 0) {
                batchPrefetchLimit = _cqlDataReaderDAO.getMultiRowPrefetchLimit();
            }
            _cqlDataReaderDAO.setMultiRowFetchSizeAndPrefetchLimit(batchFetchSize, batchPrefetchLimit);
        }


        out.printf("Use CQL for multi-gets/scans = %s/%s | FETCH_SIZE : %d | BATCH_FETCH_SIZE: %d | PREFETCH_LIMIT=%d | BATCH_PREFETCH_LIMIT=%d%n",
                _useCqlForMultiGets.get(), _useCqlForScans.get(), _cqlDataReaderDAO.getSingleRowFetchSize(),
                _cqlDataReaderDAO.getMultiRowFetchSize(), _cqlDataReaderDAO.getSingleRowPrefetchLimit(),
                _cqlDataReaderDAO.getMultiRowPrefetchLimit());
    }

    private Integer parseInt(String value, String description, PrintWriter out) {
        try {
            return Integer.parseInt(value);
        } catch (Exception e) {
            out.printf("Unable to parse value '%s' for %s: %s%n", value, description, e);
        }
        return null;
    }
}
