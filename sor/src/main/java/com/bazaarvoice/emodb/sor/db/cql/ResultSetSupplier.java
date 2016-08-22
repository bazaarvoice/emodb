package com.bazaarvoice.emodb.sor.db.cql;

import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class ResultSetSupplier {
    private static final Logger _log = LoggerFactory.getLogger(ResultSetSupplier.class);
    private final Session _session;
    private final Statement _statement;
    private final boolean _async;
    private ResultSetFuture _resultSetFuture;

    /**
     * @param async - true to execute statement asynchronously
     */
    public ResultSetSupplier(Session session, Statement statement, boolean async) {
        _session = checkNotNull(session, "session");
        _statement = checkNotNull(statement, "statement");
        _async = async;
        if (async) {
            // Execute asynchronously
            _resultSetFuture = _session.executeAsync(_statement);
        }
    }

    public ResultSet get(@Nullable String pagingState) {
        if (pagingState != null) {
            _statement.setPagingState(PagingState.fromString(pagingState));
        }
        return _session.execute(_statement);
    }

    /**
     * @return the ResultSet from ResultSetFuture set at the time of creation.
     * @throws java.lang.IllegalStateException if this is not an Async ResultSetSupplier
     */
    public ResultSet getFromFuture() {
        checkState(isAsync(), "Not An async ResultSetSupplier");
        try {
            return checkNotNull(_resultSetFuture).get();
        } catch (InterruptedException | ExecutionException e) {
            _log.warn("Async ResultSetFuture.get() failed: Retrying synchronously");
            return get(null);
        }
    }

    public boolean isAsync() {
        return _async;
    }

}
