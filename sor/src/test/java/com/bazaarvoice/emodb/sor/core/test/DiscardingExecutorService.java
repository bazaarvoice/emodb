package com.bazaarvoice.emodb.sor.core.test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of ExecutorService that silently ignores submitted jobs.
 */
public class DiscardingExecutorService extends AbstractExecutorService {

    @Override
    public void execute(Runnable command) {
        // discard
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return true;
    }

    @Override
    public void shutdown() {
        // do nothing
    }

    @Override
    public List<Runnable> shutdownNow() {
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
        return false;
    }

    @Override
    public boolean isTerminated() {
        return false;
    }
}
