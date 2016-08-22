package com.bazaarvoice.emodb.sor.db.cql;

import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.sor.db.DataReaderDAO;
import com.bazaarvoice.emodb.sor.db.astyanax.CqlDataReaderDAO;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import io.dropwizard.servlets.tasks.Task;

import java.io.PrintWriter;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * To toggle from CQL -> Astyanax or vice-versa:
 * Other options: Fetch size, async/sync multi-gets
 * <pre>
 *   curl -s -XPOST "http://localhost:8081/tasks/cql-toggle?driver=cql"
 *   curl -s -XPOST "http://localhost:8081/tasks/cql-toggle?driver=astyanax&fetchSize=15&async=true"
 * </pre>
 * To view the state of DataReaderDao:
 * <pre>
 *   curl -s -XPOST "http://localhost:8081/tasks/cql-toggle"
 * </pre>
 */
public class ToggleCqlAstyanaxTask extends Task {

    private final CqlDataReaderDAO _cqlDataReaderDAO;

    @Inject
    public ToggleCqlAstyanaxTask(TaskRegistry taskRegistry, DataReaderDAO cqlDataReaderDAO) {
        super("cql-toggle");
        checkArgument(cqlDataReaderDAO instanceof CqlDataReaderDAO, "We should be using CQL now");
        //noinspection ConstantConditions
        _cqlDataReaderDAO = (CqlDataReaderDAO) cqlDataReaderDAO;
        taskRegistry.addTask(this);
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter out) throws Exception {
        String driver = Iterables.getFirst(parameters.get("driver"), null);
        String fetchValue = Iterables.getFirst(parameters.get("fetchSize"), "-1");
        String async = Iterables.getFirst(parameters.get("async"), null);
        Optional<Boolean> asyncOption = Optional.absent();
        if (async != null) {
            asyncOption = Optional.of(Boolean.parseBoolean(async));
        }
        int fetchSize;
        try {
            fetchSize = Integer.parseInt(fetchValue);
        } catch (Exception e) {
            out.printf("Unable to parse value '%s: %s%n", fetchValue, e);
            return;
        }
        // Toggle Astyanax/Cql
        if (driver == null) {
            // Do nothing
        } else if (driver.equals("astyanax")) {
            _cqlDataReaderDAO.setAlwaysDelegateToAstyanax(true);
        } else if (driver.equals("cql")) {
            _cqlDataReaderDAO.setAlwaysDelegateToAstyanax(false);
        } else {
            out.printf("The only two options are astyanax and cql.");
        }

        // Update fetch size if needed
        if (fetchSize > 0) {
            _cqlDataReaderDAO.setFetchSize(fetchSize);
        }

        // Update async multi-gets
        if (asyncOption.isPresent()) {
            _cqlDataReaderDAO.setAsyncCqlRandomSeeks(asyncOption.get());
        }

        out.printf("Data Reader DAO is set to: %s | FETCH_SIZE : %d | BATCH_FETCH_SIZE: %d | AsyncCqlRandomSeeks: %s%n",
                _cqlDataReaderDAO.getDelegateToAstyanax() ? "Astyanax" : "CQL", _cqlDataReaderDAO.getFetchSize(),
                _cqlDataReaderDAO.getBatchFetchSize(), _cqlDataReaderDAO.getAsyncCqlRandomSeeks());
    }
}
