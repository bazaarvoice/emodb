package com.bazaarvoice.emodb.databus.core;

/**
 * Thrown when the replay job didn't complete in time and some events were already expired
 */
public class ReplayTooLateException extends RuntimeException {

    public ReplayTooLateException() {
        super("Replay completed, but was too late and some events may " +
                "have already expired before they could be replayed. This can happen if the " +
                "replay is taking too long or there are long job queues and we can't get to " +
                "the replay request in time.");
    }
}
