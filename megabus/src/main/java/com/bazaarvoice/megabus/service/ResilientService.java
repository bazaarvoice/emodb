package com.bazaarvoice.megabus.service;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static java.util.Objects.requireNonNull;


public class ResilientService extends AbstractExecutionThreadService {

    private static final Logger LOG = LoggerFactory.getLogger(ResilientService.class);

    private final Supplier<Service> _serviceFactory;
    private final Duration _restartDelay;
    private final String _serviceName;
    private final boolean _restartOnTermination;

    private final Semaphore _semaphore;

    private Service _delegate;

    public ResilientService(String serviceName, Supplier<Service> serviceFactory, Duration restartDelay,
                            boolean restartOnTermination) {
        _serviceName = requireNonNull(serviceName);
        _serviceFactory = requireNonNull(serviceFactory);
        _restartDelay = requireNonNull(restartDelay);
        _restartOnTermination = restartOnTermination;
        _semaphore = new Semaphore(0);
    }

    @Override
    protected String serviceName() {
        return _serviceName;
    }

    @Override
    protected void triggerShutdown() {
        _semaphore.release();
    }

    @Override
    protected void run() throws Exception {
        while (isRunning()) {
            try {
                _delegate = listenTo(_serviceFactory.get());
                _delegate.startAsync().awaitRunning();
                try {
                    waitForNotification();
                } finally {
                    if (_delegate.state() == State.FAILED) {
                        throw _delegate.failureCause();
                    } else {
                        _delegate.stopAsync().awaitTerminated();
                        if (!_restartOnTermination) {
                            return;
                        }
                    }
                }
            } catch (Throwable t) {
                LOG.error("Exception occurred in resilient service: {}", _serviceName, t);
            }
            finally {
                _delegate = null;
            }

            if (isRunning()) {
                sleep(_restartDelay.toNanos());
            }
        }
    }

    private void waitForNotification() throws InterruptedException {
        while (_delegate.isRunning() && isRunning()) {
            _semaphore.acquire();
        }
    }

    /** Wait for the specified amount of time or until this service is stopped, whichever comes first. */
    private synchronized void sleep(long waitNanos) throws InterruptedException {
        while (waitNanos > 0 && isRunning()) {
            long start = System.nanoTime();
            TimeUnit.NANOSECONDS.timedWait(this, waitNanos);
            waitNanos -= System.nanoTime() - start;
        }
    }

    /** Release leadership when the service terminates (normally or abnormally). */
    private Service listenTo(Service delegate) {
        delegate.addListener(new Listener() {
            @Override
            public void starting() {
                // Do nothing
            }

            @Override
            public void running() {
                // Do nothing
            }

            @Override
            public void stopping(State from) {
                // Do nothing
            }

            @Override
            public void terminated(State from) {
                _semaphore.release();
            }

            @Override
            public void failed(State from, Throwable failure) {
                _semaphore.release();
            }
        }, MoreExecutors.sameThreadExecutor());
        return delegate;
    }
}
