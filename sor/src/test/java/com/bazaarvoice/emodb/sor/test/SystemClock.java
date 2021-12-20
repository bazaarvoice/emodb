package com.bazaarvoice.emodb.sor.test;

import com.google.common.base.Throwables;

public class SystemClock {

    /**
     * Waits for at least one millisecond then returns the current time.
     */
    public static long tick() {
        long start = System.currentTimeMillis();
        long end;
        do {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Throwables.propagateIfPossible(e);
                throw new RuntimeException(e);
            }
            end = System.currentTimeMillis();
        } while (start == end);
        return end;
    }
}
