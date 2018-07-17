package com.bazaarvoice.emodb.sortedq.core;

import com.bazaarvoice.emodb.sortedq.api.Consumer;
import com.bazaarvoice.emodb.sortedq.api.SortedQueue;
import com.bazaarvoice.emodb.sortedq.db.QueueDAO;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Throwables;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import static org.testng.Assert.fail;

/**
 * Wraps a {@link PersistentSortedQueue} with logic that retries and/or reloads the queue whenever a
 * {@link SimulatedFailureException} gets thrown.
 */
class PersistentSortedQueueWithRetry implements SortedQueue {
    private final QueueDAO _dao;
    private final String _name;
    private final long _splitThresholdBytes;
    private final int _splitWorkBytes;
    private final boolean _reloadOnFailure;
    private SortedQueue _q;
    private int _numFailures;
    private MetricRegistry _metricRegistry;

    public PersistentSortedQueueWithRetry(QueueDAO dao, String name, long splitThresholdBytes, int splitWorkBytes, boolean reloadOnFailure, MetricRegistry metricRegistry) {
        _dao = dao;
        _name = name;
        _splitThresholdBytes = splitThresholdBytes;
        _splitWorkBytes = splitWorkBytes;
        _reloadOnFailure = reloadOnFailure;
        _metricRegistry = metricRegistry;
        _q = reload();
    }

    int getNumFailures() {
        return _numFailures;
    }

    private SortedQueue reload() {
        return retry(true, new Callable<SortedQueue>() {
            @Override
            public SortedQueue call() {
                SortedQueue q = new PersistentSortedQueue(_name, false, _splitThresholdBytes, _splitWorkBytes, _dao, _metricRegistry);
                if (_q != null && _q.isReadOnly()) {
                    q.setReadOnly();
                }
                return q;
            }
        });
    }

    @Override
    public boolean isReadOnly() {
        return _q.isReadOnly();  // Can't fail
    }

    @Override
    public void setReadOnly() {
        _q.setReadOnly();  // Can't fail
    }

    @Override
    public void addAll(final Collection<ByteBuffer> records) {
        retry(false, new Callable<Void>() {
            @Override
            public Void call() {
                _q.addAll(records);
                return null;
            }
        });
    }

    @Override
    public Iterator<ByteBuffer> scan(@Nullable final ByteBuffer fromInclusive, final long limit) {
        // Scan has no side effects and an Iterator is unusable once it has thrown an exception.
        // So there isn't much point to testing scan() w/retry.
        throw new UnsupportedOperationException();
    }

    @Override
    public void drainTo(final Consumer consumer, final long limit) {
        drainTo(consumer, limit, null);
    }

    @Override
    public void drainTo(final Consumer consumer, final long limit, @Nullable final Duration timeout) {
        retry(false, new Callable<Void>() {
            private long _remaining = limit;

            @Override
            public Void call() {
                if (_remaining > 0) {
                    _q.drainTo(new Consumer() {
                        @Override
                        public void consume(List<ByteBuffer> records) {
                            _remaining -= records.size();
                            consumer.consume(records);
                        }
                    }, _remaining, timeout);
                }
                return null;
            }
        });
    }

    @Override
    public void clear() {
        retry(false, new Callable<Void>() {
            @Override
            public Void call() {
                _q.clear();
                return null;
            }
        });
    }

    @Override
    public long sizeEstimate() {
        return retry(false, new Callable<Long>() {
            @Override
            public Long call() {
                return _q.sizeEstimate();
            }
        });
    }

    @Override
    public boolean isEmpty() {
        return retry(false, new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return _q.isEmpty();
            }
        });
    }

    private <T> T retry(boolean skipReload, Callable<T> task) {
        for (int attempt = 1; ; attempt++) {
            try {
                return task.call();
            } catch (SimulatedFailureException e) {
                _numFailures++;
                if (attempt == 1000) {
                    fail("Too many retry attempts.", e);
                }
                if (_reloadOnFailure && !skipReload) {
                    _q = reload();
                }
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
