package com.bazaarvoice.emodb.databus.api;

/**
 * Request object for moving all content from one databus subscription into another, as used by
 * {@link Databus#moveAsync(MoveSubscriptionRequest)}.  The only required attributes are "from" and "to.
 * 
 * @see Databus#moveAsync(MoveSubscriptionRequest)
 */
public class MoveSubscriptionRequest {

    private String _from;
    private String _to;
    private DatabusEventTracerSpec _tracer;

    /**
     * Empty constructor.  If used the caller must at some point call {@link #from(String)} and {@link #to(String)}.
     */
    public MoveSubscriptionRequest() {
    }

    public MoveSubscriptionRequest(String from, String to) {
        from(from);
        to(to);
    }

    public MoveSubscriptionRequest from(String from) {
        _from = from;
        return this;
    }

    public MoveSubscriptionRequest to(String to) {
        _to = to;
        return this;
    }

    public MoveSubscriptionRequest withTracing(DatabusEventTracerSpec tracer) {
        _tracer = tracer;
        return this;
    }

    public String getFrom() {
        return _from;
    }

    public String getTo() {
        return _to;
    }

    public DatabusEventTracerSpec getTracer() {
        return _tracer;
    }
}
