package com.bazaarvoice.emodb.sor.db.cql;

import com.bazaarvoice.emodb.common.cassandra.CqlDriverConfiguration;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import io.dropwizard.servlets.tasks.Task;

import java.io.PrintWriter;

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

    private final CqlDriverConfiguration _cqlDriverConfiguration;
    private final Supplier<Boolean> _useCqlForMultiGets;
    private final Supplier<Boolean> _useCqlForScans;

    @Inject
    public SorCqlSettingsTask(TaskRegistry taskRegistry, CqlDriverConfiguration cqlDriverConfiguration,
                              @CqlForMultiGets Supplier<Boolean> useCqlForMultiGets,
                              @CqlForScans Supplier<Boolean> useCqlForScans) {
        super("sor-cql-settings");
        _cqlDriverConfiguration = cqlDriverConfiguration;
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

        // Update fetch sizes and prefetch limits if needed
        if (fetchSize > 0 || prefetchLimit >= 0) {
            if (fetchSize > 0) {
                _cqlDriverConfiguration.setSingleRowFetchSize(fetchSize);
            }
            if (prefetchLimit >= 0) {
                _cqlDriverConfiguration.setSingleRowPrefetchLimit(prefetchLimit);
            }
        }
        if (batchFetchSize > 0 || batchPrefetchLimit >= 0) {
            if (batchFetchSize > 0) {
                _cqlDriverConfiguration.setMultiRowFetchSize(batchFetchSize);
            }
            if (batchPrefetchLimit >= 0) {
                _cqlDriverConfiguration.setMultiRowPrefetchLimit(batchPrefetchLimit);
            }
        }

        out.printf("Use CQL for multi-gets/scans = %s/%s.  To change these values use the \"sor-cql-driver\" task.%n%n",
                _useCqlForMultiGets.get(), _useCqlForScans.get());

        out.printf("FETCH_SIZE : %d | BATCH_FETCH_SIZE: %d | PREFETCH_LIMIT=%d | BATCH_PREFETCH_LIMIT=%d%n",
                _cqlDriverConfiguration.getSingleRowFetchSize(), _cqlDriverConfiguration.getMultiRowFetchSize(),
                _cqlDriverConfiguration.getSingleRowPrefetchLimit(), _cqlDriverConfiguration.getMultiRowPrefetchLimit());
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
