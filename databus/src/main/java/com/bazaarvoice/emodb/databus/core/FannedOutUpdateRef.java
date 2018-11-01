package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.sor.core.UpdateRef;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class FannedOutUpdateRef {

    @NotNull private final UpdateRef _updateRef;
    @NotNull private final Set<String> _subscriptionNames;

    public FannedOutUpdateRef(@NotNull UpdateRef updateRef,  @NotNull Set<String> subscriptionNames) {
        _updateRef = updateRef;
        _subscriptionNames = new HashSet<>(subscriptionNames);
    }

    public UpdateRef getUpdateRef() { return _updateRef; }

    public Set<String> getSubscriptionNames() { return _subscriptionNames; }

    @Override public String toString() {
        StringBuilder sb = new StringBuilder();
        if (_updateRef != null) {
            sb.append("{ _updateRef: " + _updateRef.toString());
        } else {
            sb.append("{ _updateRef: NULL");
        }

        if (_updateRef != null) {
            sb.append(", _subscriptionNames: " + _subscriptionNames.toString() + " }");
        } else {
            sb.append(", _subscriptionNames: NULL }");
        }

        return sb.toString();
    }

}
