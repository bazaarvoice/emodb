package com.bazaarvoice.emodb.sor.core;

public interface DatabusEventWriter {
    void writeEvent(UpdateIntentEvent event);
}
