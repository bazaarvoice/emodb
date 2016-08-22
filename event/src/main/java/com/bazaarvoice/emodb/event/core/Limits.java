package com.bazaarvoice.emodb.event.core;

import org.joda.time.Duration;

public abstract class Limits {
    /** Cap the total # claims per channel so badly behaved pollers (poll but never ack) can't overload the system. */
    public static final int MAX_CLAIMS_OUTSTANDING = 20000;

    /** Cap the number of events that peek can fetch at once. */
    public static final int MAX_PEEK_LIMIT = 5000;

    /** Cap the number of events that poll can fetch at once. */
    public static final int MAX_POLL_LIMIT = 1000;

    /** Cap the amount of time a claim can be held. */
    public static final Duration MAX_CLAIM_TTL = Duration.standardHours(1);
}
