package test.client.commons.utils;

import com.bazaarvoice.emodb.common.api.ServiceUnavailableException;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class RetryUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetryUtils.class);

    public static void withRetryOnServiceUnavailable(Runnable runnable) {
        withRetryOnServiceUnavailable(() -> {
            runnable.run();
            return null;
        });
    }

    public static <T> T withRetryOnServiceUnavailable(Callable<T> callable) {
        final int attempts = 5;
        int attempt = 0;
        do {
            try {
                return callable.call();
            } catch (ServiceUnavailableException e) {
                LOGGER.warn(String.format("ServiceUnavailableException, Attempt: %s", attempt), e);
                snooze(5 + new Random().nextInt(9));
            } catch (Exception e) {
                LOGGER.error(String.format("Exception, Attempt: %s", attempt), e);
                Throwables.propagateIfPossible(e);
                throw new RuntimeException(e);
            }
        } while (++attempt <= attempts);

        // Unreachable, but must return something
        throw new RuntimeException("Failed to execute callable");
    }

    public static void snooze(long seconds) {
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(seconds));
        } catch (InterruptedException e) {
            LOGGER.error("Something went wrong while snoozing.", e);
            throw new RuntimeException(e);
        }
    }
}
