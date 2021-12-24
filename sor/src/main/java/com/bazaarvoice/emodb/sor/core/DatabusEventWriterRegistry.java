package com.bazaarvoice.emodb.sor.core;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * This is used to send events to the databus prior to writing to the system of record.
 * This databus should register as an {@link DatabusEventWriter} with this registry and then take events synchronously.
 * If an event fails to be written to the databus, then it should propagate and exception and prevent Emo from writing
 * to the system of record.
 */
public class DatabusEventWriterRegistry {

    private DatabusEventWriter _databusWriter;
    private boolean _hasRegistered;

    /**
     * The initial databus registration is a no-op which will discard all events sent to it until the databus registers.
     * If the databus never registers, than events will continue to be discarded. It is okay for events to be discarded
     * in certain Emo service modes, such as stash. However, it is certainly not okay during normal web operation.
     */
    public DatabusEventWriterRegistry() {
        _hasRegistered = false;
        _databusWriter = event -> {};
    }

    public void registerDatabusEventWriter(DatabusEventWriter databusWriter) {
        requireNonNull(databusWriter);
        checkArgument(!_hasRegistered, "Databus Event Writer already registered");
        _databusWriter = databusWriter;
        _hasRegistered = true;
    }

    public DatabusEventWriter getDatabusWriter() {
        return _databusWriter;
    }
}
