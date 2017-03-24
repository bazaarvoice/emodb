package com.bazaarvoice.emodb.databus.api;

import java.util.Date;

/**
 * Request object for replaying all content from up to the past two days into a subscription, as used by
 * {@link Databus#replayAsync(ReplaySubscriptionRequest)}.  The only required attribute is "subscription".
 *
 * @see Databus#replayAsync(ReplaySubscriptionRequest)
 */
public class ReplaySubscriptionRequest {

    private String _subscription;
    private Date _since;
    private DatabusEventTracerSpec _tracer;

    public ReplaySubscriptionRequest() {
        // empty
    }

    public ReplaySubscriptionRequest(String subscription) {
        subscription(subscription);
    }

    public ReplaySubscriptionRequest subscription(String subscription) {
        _subscription = subscription;
        return this;
    }

    public ReplaySubscriptionRequest since(Date since) {
        _since = since;
        return this;
    }

    public ReplaySubscriptionRequest withTracing(DatabusEventTracerSpec tracer) {
        _tracer = tracer;
        return this;
    }

    public String getSubscription() {
        return _subscription;
    }

    public Date getSince() {
        return _since;
    }

    public DatabusEventTracerSpec getTracer() {
        return _tracer;
    }
}
