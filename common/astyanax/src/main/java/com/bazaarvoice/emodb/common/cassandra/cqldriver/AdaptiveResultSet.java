package com.bazaarvoice.emodb.common.cassandra.cqldriver;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.FrameTooLongException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.utils.MoreFutures;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Result set implementation which adapts the fetch size down in response to queries which:
 *
 * <ol>
 *     <li>Return a page larger than the maximum frame size, 256MB, or</li>
 *     <li>Timeout reading too many results</li>
 * </ol>
 */
public class AdaptiveResultSet implements ResultSet {

    private final static Logger _log = LoggerFactory.getLogger(AdaptiveResultSet.class);

    // Minimum fetch size.  No adaptations will be made below this level.
    private final static int MIN_FETCH_SIZE = 10;
    // Cap the number of times the result set fetch size can be adapted downward.
    private final static int MAX_ADAPTATIONS = 5;

    /**
     * Executes a query asychronously, dynamically adjusting the fetch size down if necessary.
     */
    public static ListenableFuture<ResultSet> executeAdaptiveQueryAsync(Session session, Statement statement, int fetchSize) {
        return executeAdaptiveQueryAsync(session, statement, fetchSize, MAX_ADAPTATIONS);
    }

    private static ListenableFuture<ResultSet> executeAdaptiveQueryAsync(Session session, Statement statement, int fetchSize,
                                                                         int remainingAdaptations) {

        statement.setFetchSize(fetchSize);

        ResultSetFuture rawFuture = session.executeAsync(statement);

        // Lazily wrap the result set from the async result with an AdaptiveResultSet
        ListenableFuture<ResultSet> adaptiveFuture = Futures.transform(rawFuture, new Function<ResultSet, ResultSet>() {
            @Override
            public ResultSet apply(ResultSet resultSet) {
                return new AdaptiveResultSet(session, resultSet, remainingAdaptations);
            }
        });

        return Futures.catchingAsync(adaptiveFuture, Throwable.class, t -> {
            if (isAdaptiveException(t) && remainingAdaptations > 0 && fetchSize > MIN_FETCH_SIZE) {
                // Try again with half the fetch size
                int reducedFetchSize = Math.max(fetchSize / 2, MIN_FETCH_SIZE);
                _log.debug("Repeating previous query with fetch size {} due to {}", reducedFetchSize, t.getMessage());
                return executeAdaptiveQueryAsync(session, statement, reducedFetchSize, remainingAdaptations - 1);
            }
            throw new RuntimeException(t);
        }, Executors.newSingleThreadExecutor());
    }

    /**
     * Executes a query sychronously, dynamically adjusting the fetch size down if necessary.
     */
    public static ResultSet executeAdaptiveQuery(Session session, Statement statement, int fetchSize) {
        int remainingAdaptations = MAX_ADAPTATIONS;
        while (true) {
            try {
                statement.setFetchSize(fetchSize);
                ResultSet resultSet = session.execute(statement);
                return new AdaptiveResultSet(session, resultSet, remainingAdaptations);
            } catch (Throwable t) {
                if (isAdaptiveException(t) && --remainingAdaptations != 0 && fetchSize > MIN_FETCH_SIZE) {
                    // Try again with half the fetch size
                    fetchSize = Math.max(fetchSize / 2, MIN_FETCH_SIZE);
                    _log.debug("Repeating previous query with fetch size {} due to {}", fetchSize, t.getMessage());
                } else {
                    throw Throwables.propagate(t);
                }
            }
        }
    }

    /**
     * Returns true if the exception is one which indicates that the frame size may be too large, false otherwise.
     */
    private static boolean isAdaptiveException(Throwable t) {
        if (t instanceof FrameTooLongException) {
            return true;
        }

        if (t instanceof NoHostAvailableException) {
            // If the issue on every host is adaptive then the exception is adaptive
            Collection<Throwable> hostExceptions = ((NoHostAvailableException) t).getErrors().values();
            return !hostExceptions.isEmpty() && hostExceptions.stream().allMatch(AdaptiveResultSet::isAdaptiveException);
        }

        return false;
    }

    private final Session _session;
    private ResultSet _delegate;
    private Iterator<Row> _fetchedResults = Collections.emptyIterator();
    private volatile ResultSet _delegateWithPrefetchFailure;
    private volatile Throwable _prefetchFailure;
    private int _remainingAdaptations;

    private AdaptiveResultSet(Session session, ResultSet delegate, int remainingAdaptations) {
        _session = session;
        _delegate = delegate;
        _remainingAdaptations = remainingAdaptations;
    }

    @Override
    public Row one() {
        // If we've already identified pre-fetched rows that can be read locally then return the next row.
        if (_fetchedResults.hasNext()) {
            return _fetchedResults.next();
        }

        // Determine how many rows are available without fetching, if any.  This can happen if a call to
        // fetchMoreResults() made more results locally available since _fetchedResults was created.
        int availableWithoutFetching = _delegate.getAvailableWithoutFetching();
        if (availableWithoutFetching != 0) {
            // Create an iterator for these rows to return them efficiently since we know returning them
            // will never throw an adaptive exception.
            _fetchedResults = Iterators.limit(_delegate.iterator(), availableWithoutFetching);
            return _fetchedResults.next();
        }

        // At this point either the result set is exhausted or the next row requires fetching more results.  Determining
        // either of these may potentially raise an exception which requires adapting the fetch size.

        Throwable fetchException;

        // If an asynchronous pre-fetch from a prior call to fetchMoreResults() failed for an adaptive reason then
        // don't try again with the current fetch size.
        if (_delegateWithPrefetchFailure == _delegate && _prefetchFailure != null) {
            fetchException = _prefetchFailure;
            _delegateWithPrefetchFailure = null;
            _prefetchFailure = null;
        } else {
            try {
                return _delegate.one();
            } catch (Throwable t) {
                fetchException = t;
            }
        }

        // This code is only reachable if there was an exception fetching more rows.  If appropriate reduce the fetch
        // size and try again, otherwise propagate the exception.
        if (!reduceFetchSize(fetchException)) {
            throw Throwables.propagate(fetchException);
        }

        // Call again to return the next row.
        return one();
    }

    /**
     * Reduces the fetch size and retries the query.  Returns true if the query succeeded, false if the root cause
     * of the exception does not indicate a frame size issue, if the frame size cannot be adjusted down any further,
     * or if the retried query fails for an unrelated reason.
     */
    private boolean reduceFetchSize(Throwable reason) {
        if (!isAdaptiveException(reason) || --_remainingAdaptations == 0) {
            return false;
        }

        ExecutionInfo executionInfo = _delegate.getExecutionInfo();
        Statement statement = executionInfo.getStatement();
        PagingState pagingState = executionInfo.getPagingState();
        int fetchSize = statement.getFetchSize();

        while (fetchSize > MIN_FETCH_SIZE) {
            fetchSize = Math.max(fetchSize / 2, MIN_FETCH_SIZE);
            _log.debug("Retrying query at next page with fetch size {} due to {}", fetchSize, reason.getMessage());
            statement.setFetchSize(fetchSize);
            statement.setPagingState(pagingState);
            try {
                _delegate = _session.execute(statement);
                return true;
            } catch (Throwable t) {
                // Exit the adaptation loop if the exception isn't one where adapting further may help
                if (!isAdaptiveException(t) || --_remainingAdaptations == 0) {
                    return false;
                }
            }
        }

        return false;
    }

    @Override
    public Iterator<Row> iterator() {
        return new AbstractIterator<Row>() {
            @Override
            protected Row computeNext() {
                Row next = one();
                if (next != null) {
                    return next;
                }
                return endOfData();
            }
        };
    }

    @Override
    public List<Row> all() {
        return StreamSupport.stream(spliterator(), false).collect(Collectors.toList());
    }

    @Override
    public ListenableFuture<ResultSet> fetchMoreResults() {
        final ResultSet delegate = _delegate;

        // If we've already tried to pre-fetch for this delegate and ran into frame size issues then don't try again.
        if (_delegateWithPrefetchFailure == delegate) {
            return Futures.immediateFuture(this);
        }

        // Change the returned future to contain this instance instead of the delegate
        ListenableFuture<ResultSet> future = Futures.transform(delegate.fetchMoreResults(), new Function<ResultSet, ResultSet>() {
            @Override
            public ResultSet apply(ResultSet ignore) {
                return AdaptiveResultSet.this;
            }
        });

        Futures.addCallback(future, new MoreFutures.FailureCallback<ResultSet>() {
            @Override
            public void onFailure(Throwable t) {
                // The async pre-fetch has failed.  Check if the root cause is adaptive.
                if (isAdaptiveException(t)) {
                    // Future:  Optimize to pre-fetch the next delegate immediately.  For now simply record that
                    // we shouldn't try to pre-fetch again for this delegate.  The frame size will be adjusted
                    // synchronously after all available rows have been consumed.

                    _prefetchFailure = t;
                    _delegateWithPrefetchFailure = delegate;
                }
            }
        });

        return future;
    }

    // Remaining methods require no additional logic beyond forwarding calls to the ResultSet delegate.

    @Override
    public ColumnDefinitions getColumnDefinitions() {
        return _delegate.getColumnDefinitions();
    }

    @Override
    public boolean wasApplied() {
        return _delegate.wasApplied();
    }

    @Override
    public boolean isExhausted() {
        return _delegate.isExhausted();
    }

    @Override
    public boolean isFullyFetched() {
        return _delegate.isFullyFetched();
    }

    @Override
    public int getAvailableWithoutFetching() {
        return _delegate.getAvailableWithoutFetching();
    }

    @Override
    public ExecutionInfo getExecutionInfo() {
        return _delegate.getExecutionInfo();
    }

    @Override
    public List<ExecutionInfo> getAllExecutionInfo() {
        return _delegate.getAllExecutionInfo();
    }
}
