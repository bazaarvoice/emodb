package com.bazaarvoice.emodb.sor.core;

import java.util.Collection;
import java.util.EventObject;

public class UpdateIntentEvent extends EventObject {
    private final Collection<UpdateRef> _updateRefs;

    public UpdateIntentEvent(Object source, Collection<UpdateRef> updateRefs) {
        super(source);
        _updateRefs = updateRefs;
    }

    public Collection<UpdateRef> getUpdateRefs() {
        return _updateRefs;
    }
}
