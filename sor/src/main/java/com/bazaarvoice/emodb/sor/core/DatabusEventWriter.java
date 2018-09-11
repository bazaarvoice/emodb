package com.bazaarvoice.emodb.sor.core;

/**
 * Interface that should register with {@link DatabusEventWriterRegistry} and write events to the databus prior to
 * writing them to the system of record. If an event fails to be written to the databus, then it should propagate and
 * exception and prevent Emo from writing to the system of record.
 */
public interface DatabusEventWriter {


    void writeEvent(UpdateIntentEvent event);
}
