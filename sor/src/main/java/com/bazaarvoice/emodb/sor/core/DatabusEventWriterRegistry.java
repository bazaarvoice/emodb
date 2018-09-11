package com.bazaarvoice.emodb.sor.core;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DatabusEventWriterRegistry {

    private DatabusEventWriter _databusWriter;
    private boolean _hasRegistered;

    public DatabusEventWriterRegistry() {
        _hasRegistered = false;
        _databusWriter = event -> {};
    }

    public void registerDatabusEventWriter(DatabusEventWriter databusWriter) {
        checkNotNull(databusWriter);
        checkArgument(!_hasRegistered);
        _databusWriter = databusWriter;
        _hasRegistered = true;
    }

    public DatabusEventWriter getDatabusWriter() {
        return _databusWriter;
    }

    public boolean hasRegistered() {
        return _hasRegistered;
    }
}
