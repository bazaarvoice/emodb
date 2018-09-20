package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.sor.core.UpdateRef;

import javax.validation.constraints.NotNull;
import java.util.HashSet;
import java.util.Set;

public final class ResolvedUpdateRef {

    @NotNull private final UpdateRef _updateRef;
    @NotNull private final Set<String> _subscriptionNames;
    @NotNull private final String _document;

    public ResolvedUpdateRef(@NotNull UpdateRef updateRef, @NotNull Set<String> subscriptionNames, @NotNull String document) {
        _updateRef = updateRef;
        _subscriptionNames = new HashSet<>(subscriptionNames);
        _document = document;
    }

    public UpdateRef getUpdateRef() { return _updateRef; }

    public Set<String> getSubscriptionNames() { return _subscriptionNames; }

    public String getDocument() { return _document; }

}
