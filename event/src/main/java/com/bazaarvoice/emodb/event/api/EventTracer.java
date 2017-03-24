package com.bazaarvoice.emodb.event.api;

import java.io.Closeable;
import java.nio.ByteBuffer;

/**
 * Some operations, such as moving events from one source to another, are black boxed by nature such that the caller
 * gets little insight into the operation other than when it completes.  Where supported, some operations take an optional
 * EventTracer which can be used to trace those events which are acted upon by the operation.  What happens to the
 * events which are traced -- such as writing to a log file -- is up to the implementation.
 */
public interface EventTracer extends Closeable {

    /**
     * Primary interface for tracing that an event is being acted upon.
     * @param source Brief string description for how the event got included in the operation
     * @param data Context-specific event data
     */
    void trace(String source, ByteBuffer data);
}
