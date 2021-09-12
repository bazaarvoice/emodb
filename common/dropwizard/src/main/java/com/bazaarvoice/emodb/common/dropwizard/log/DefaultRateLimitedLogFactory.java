package com.bazaarvoice.emodb.common.dropwizard.log;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.ExecutorServiceManager;
import io.dropwizard.util.Duration;
import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Limits the rate that errors are logged for situations where, if something goes wrong, it's likely to go wrong many
 * times per second and all we're interested in is whether or not the error is still occurring.
 * <p>
 * This is similar to logback's DuplicateMessageFilter except that it periodically reports summary statistics about
 * duplicate messages instead of just dropping them completely.
 */
public class DefaultRateLimitedLogFactory implements RateLimitedLogFactory {
    private final ScheduledExecutorService _executor;
    private final Duration _interval;
    private final LoadingCache<Message, Message> _cache;

    @Inject
    public DefaultRateLimitedLogFactory(LifeCycleRegistry lifeCycle) {
        this(defaultExecutor(lifeCycle), Duration.seconds(30));
    }

    @VisibleForTesting
    DefaultRateLimitedLogFactory(ScheduledExecutorService executor, Duration interval) {
        _executor = checkNotNull(executor, "executor");
        _interval = checkNotNull(interval, "interval");

        // After the last access we (1) hold the error up to 30 seconds before reporting it, then (2) wait to see if
        // any more instances of the error occur, after the (3) third 30-second interval of no more access we can be
        // confident that we can safely stop tracking the error and expire it from the cache.  Hence "interval * 3".
        _cache = CacheBuilder.newBuilder()
                .expireAfterAccess(interval.getQuantity() * 3, interval.getUnit())
                .build(CacheLoader.from(Functions.<Message>identity()));
        _executor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                _cache.cleanUp();
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    private static ScheduledExecutorService defaultExecutor(LifeCycleRegistry lifeCycle) {
        String nameFormat = "RateLimitedLog-%d";
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(nameFormat).setDaemon(true).build();
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1, threadFactory);
        lifeCycle.manage(new ExecutorServiceManager(executor, Duration.seconds(5), nameFormat));
        return executor;
    }

    @Override
    public RateLimitedLog from(final Logger log) {
        checkNotNull(log, "log");
        return new RateLimitedLog() {
            @Override
            public void error(Throwable t, String message, Object... args) {
                _cache.getUnchecked(new Message(log, message)).error(t, args);
            }
        };
    }

    private class Message implements Runnable {
        private final Logger _log;
        private final String _message;
        private long _count;
        private Throwable _lastThrowable;
        private Object[] _lastArgs;
        private boolean _scheduled;

        private Message(Logger log, String message) {
            _log = log;
            _message = message;
        }

        public synchronized void error(Throwable t, Object... args) {
            if (!_scheduled) {
                checkState(_count == 0);

                // The first time we see this error, log it immediately.
                _log.error(MessageFormatter.arrayFormat(_message, args).getMessage(), t);

                // Don't log this error again until 30 seconds have passed.
                _executor.schedule(this, _interval.getQuantity(), _interval.getUnit());
                _scheduled = true;
            } else {
                // We've logged this error recently.  Just bump a counter and we'll output to the log on schedule.
                _count++;
                _lastThrowable = t;
                _lastArgs = args;
            }
        }

        private synchronized void report() {
            if (_count > 0) {
                // Log a summary of the last 30 seconds of errors + details of the last error instance.
                String message = MessageFormatter.arrayFormat(_message, _lastArgs).getMessage();
                _log.error("Encountered {} {} within the last {}: {}",
                        _count, _count == 1 ? "error" : "errors", _interval, message, _lastThrowable);

                // Reset counters
                _count = 0;
                _lastThrowable = null;
                _lastArgs = null;

                // Schedule so we don't log this particular error again for another 30 seconds.
                _executor.schedule(this, _interval.getQuantity(), _interval.getUnit());
                _scheduled = true;
            } else {
                // It has been at least 30 seconds since we saw this error.  Stop tracking it.
                _scheduled = false;
            }
        }

        @Override
        public void run() {
            report();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Message)) {
                return false;
            }
            Message message = (Message) o;
            return _log.equals(message._log) && Objects.equal(_message, message._message);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(_log, _message);
        }
    }
}
