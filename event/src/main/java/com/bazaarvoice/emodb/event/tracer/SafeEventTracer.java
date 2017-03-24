package com.bazaarvoice.emodb.event.tracer;

import com.bazaarvoice.emodb.event.api.EventTracer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link EventTracer} implementation which catches all exceptions on each tracer call, logs the exception, then
 * returns control to the caller.  By delegating an EventTracer to a SafeEventTracer instance the caller can be assured
 * that any tracing failures will not cause the more important overall operation to fail.
 */
public class SafeEventTracer implements EventTracer {

    private final EventTracer _eventTracer;
    private final Logger _log;

    public SafeEventTracer(EventTracer eventTracer) {
        this(eventTracer, null);
    }

    public SafeEventTracer(EventTracer eventTracer, @Nullable Logger log) {
        _eventTracer = checkNotNull(eventTracer, "eventTracer");
        _log = log;
    }

    @Override
    public void trace(String source, ByteBuffer data) {
        try {
            _eventTracer.trace(source, data.asReadOnlyBuffer());
        } catch (Exception e) {
            if (_log != null) {
                _log.warn("Failed to trace event: data={}", ByteBufferUtil.bytesToHex(data), e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            _eventTracer.close();
        } catch (Exception e) {
            if (_log != null) {
                _log.warn("Failed to close tracer", e);
            }
        }
    }
}
