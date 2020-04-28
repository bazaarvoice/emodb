package com.bazaarvoice.emodb.blob.db.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.bazaarvoice.emodb.blob.db.s3.config.S3ClientConfiguration;
import com.google.common.base.Throwables;
import com.google.common.reflect.AbstractInvocationHandler;
import com.google.common.reflect.Reflection;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class S3RateLimiter {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3RateLimiter.class);

    private final int maxAttempts;
    private final SharedRateLimiter _l;

    public S3RateLimiter(Clock clock, S3ClientConfiguration.RateLimitConfiguration rateLimitConfiguration) {
        _l = new SharedRateLimiter(clock, Objects.requireNonNull(rateLimitConfiguration));
        maxAttempts = rateLimitConfiguration.getMaxAttempts();
    }

    public AmazonS3 rateLimit(final AmazonS3 delegate) {
        return Reflection.newProxy(AmazonS3.class, new AbstractInvocationHandler() {

            @Override
            protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
                int attempts = 0;
                _l.maybeIncreaseRateLimit();

                while (true) {
                    try {
                        _l.beforeS3Method();
                        return method.invoke(delegate, args);
                    } catch (InvocationTargetException ite) {
                        Throwable t = ite.getTargetException();
                        attempts += 1;

                        if (attempts == maxAttempts || !_l.checkException(t, method)) {
                            LOGGER.warn("S3 exception being raised to caller after {} attempts", attempts, t);
                            throw _l.asDeclaredThrowable(t, method);
                        }
                    }
                }
            }
        });
    }

    private static final class SharedRateLimiter {

        private final Clock _clock;

        private Duration decreaseCooldown;
        private Duration increaseCooldown;
        private double maxRateLimit;
        private double minRateLimit;

        // Start with a fairly loose rate limiter
        private volatile double _rateLimit;
        private volatile RateLimiter _rateLimiter;
        private volatile Instant _endIncreaseCooldownPeriod = Instant.MIN;
        private volatile Instant _endDecreaseCooldownPeriod = Instant.MIN;

        SharedRateLimiter(Clock clock, S3ClientConfiguration.RateLimitConfiguration rateLimitConfiguration) {
            _clock = Objects.requireNonNull(clock);
            decreaseCooldown = Objects.requireNonNull(rateLimitConfiguration.getDecreaseCooldown());
            increaseCooldown = Objects.requireNonNull(rateLimitConfiguration.getIncreaseCooldown());
            maxRateLimit = rateLimitConfiguration.getMaxRateLimit();
            minRateLimit = rateLimitConfiguration.getMinRateLimit();

            _rateLimit = maxRateLimit;
            _rateLimiter = RateLimiter.create(maxRateLimit);
        }

        void beforeS3Method() {
            _rateLimiter.acquire();
        }

        boolean checkException(Throwable t, Method method) throws Throwable {
            if (isRequestRateExceededException(t)) {
                decreaseRateLimit();
                waitForRetry(t, method);
                return true;
            }
            return false;
        }

        private boolean isRequestRateExceededException(Throwable t) {
            if (t instanceof AmazonS3Exception) {
                AmazonS3Exception e = (AmazonS3Exception) t;
                // Several ways AWS communicates rate limit exceeded: 503 status codes and "SlowDown" error codes.
                // Check for either.
                return e.getStatusCode() == HttpStatus.SC_SERVICE_UNAVAILABLE ||
                        (e.getErrorCode() != null && e.getErrorCode().toLowerCase().contains("slowdown"));

            }
            return false;
        }

        private synchronized void decreaseRateLimit() {
            Instant now = _clock.instant();
            if (now.isAfter(_endDecreaseCooldownPeriod)) {
                // Decrease by half
                _rateLimit = Math.max(_rateLimit / 2, minRateLimit);
                _rateLimiter = RateLimiter.create(_rateLimit, 1, TimeUnit.SECONDS);
                _endDecreaseCooldownPeriod = now.plus(decreaseCooldown);
                _endIncreaseCooldownPeriod = now.plus(increaseCooldown);
                LOGGER.info("S3 rate limit decreased to {}", _rateLimit);
            }
        }

        void maybeIncreaseRateLimit() {
            if (_rateLimit < maxRateLimit) {
                synchronized (this) {
                    if (_rateLimit < maxRateLimit) {
                        Instant now = _clock.instant();
                        if (now.isAfter(_endIncreaseCooldownPeriod)) {
                            // Increase by 25%
                            _rateLimit = Math.min(_rateLimit * 1.25, maxRateLimit);
                            _rateLimiter = RateLimiter.create(_rateLimit, 1, TimeUnit.SECONDS);
                            _endIncreaseCooldownPeriod = now.plus(increaseCooldown);
                            LOGGER.info("S3 rate limit increased to {}", _rateLimit);
                        }
                    }
                }
            }
        }

        private void waitForRetry(Throwable t, Method method) throws Throwable {
            // Backoff for a random amount of time between 1 and 5 seconds
            try {
                Thread.sleep(1000 + (int) (Math.random() * 4000));
            } catch (InterruptedException e) {
                // On interrupt don't keep retrying, just throw the original exception
                LOGGER.warn("S3 operation interrupted while retrying rate limited request");
                throw asDeclaredThrowable(t, method);
            }
        }

        private Throwable asDeclaredThrowable(Throwable t, Method method) throws Throwable {
            // Do our best to re-throw the exception as it is declared
            for (Class<?> declaredException : method.getExceptionTypes()) {
                // noinspection unchecked
                Throwables.propagateIfInstanceOf(t, (Class<? extends Throwable>) declaredException);
            }
            throw Throwables.propagate(t);
        }
    }
}
