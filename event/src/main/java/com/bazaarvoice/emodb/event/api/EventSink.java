package com.bazaarvoice.emodb.event.api;

public interface EventSink {
    enum Status {
        /** Send more events. */
        ACCEPTED_CONTINUE,
        /** Stop sending events. */
        ACCEPTED_STOP,
        /** Stop sending events, release any claims on the event just sent. */
        REJECTED_STOP,
    }

    /**
     * Returns an estimate of the limit remaining. This must return zero or a negative number if the last call to
     * {@code accept} returned a "stop" status.
     */
    int remaining();

    /** Returns a "continue" status to keep searching for more events, a "stop" status to stop searching for events. */
    Status accept(EventData event);
}
