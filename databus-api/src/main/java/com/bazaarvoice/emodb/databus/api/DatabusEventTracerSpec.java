package com.bazaarvoice.emodb.databus.api;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Specification for tracing events for databus move and replay operations.  When tracing each event which is moved
 * or replayed is inserted into a trace log.  The nature of the trace log is dependent on the spec.
 */
@JsonTypeInfo (use=JsonTypeInfo.Id.NAME, include=JsonTypeInfo.As.PROPERTY, property="type")
@JsonSubTypes({
        @JsonSubTypes.Type(value=BlobCSVEventTracerSpec.class)
})
public interface DatabusEventTracerSpec {
}
