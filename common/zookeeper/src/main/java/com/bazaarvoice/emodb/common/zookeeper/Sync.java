package com.bazaarvoice.emodb.common.zookeeper;

import com.google.common.base.Throwables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Sync {
    /**
     * Performs a blocking sync operation.  Returns true if the sync completed normally, false if it timed out or
     * was interrupted.
     */
    public static boolean synchronousSync(CuratorFramework curator, Duration timeout) {
        try {
            // Curator sync() is always a background operation.  Use a latch to block until it finishes.
            final CountDownLatch latch = new CountDownLatch(1);
            curator.sync().inBackground(new BackgroundCallback() {
                @Override
                public void processResult(CuratorFramework curator, CuratorEvent event) throws Exception {
                    if (event.getType() == CuratorEventType.SYNC) {
                        latch.countDown();
                    }
                }
            }).forPath(curator.getNamespace().isEmpty() ? "/" : "");

            // Wait for sync to complete.
            return latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        } catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
