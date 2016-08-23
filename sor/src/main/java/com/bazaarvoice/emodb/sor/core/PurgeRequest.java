package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.sor.api.Audit;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkNotNull;


public class PurgeRequest  {

    private final Audit _audit;
    private final String _table;

    @JsonCreator
    public PurgeRequest(@JsonProperty("table") String table, @JsonProperty("audit") Audit audit) {
        _audit = checkNotNull(audit, "audit");
        _table = checkNotNull(table, "table");
    }

    public Audit getAudit() {
        return _audit;
    }

    public String getTable() {
        return _table;
    }
}

