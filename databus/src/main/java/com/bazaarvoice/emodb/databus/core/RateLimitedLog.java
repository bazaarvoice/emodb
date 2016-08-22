package com.bazaarvoice.emodb.databus.core;

public interface RateLimitedLog {
    /**
     * Logs the specified error message.  If the same message (not counting arguments) has been logged recently, this
     * error may be consolidated into a single error in the logs.
     */
    void error(Throwable e, String message, Object... args);
}
