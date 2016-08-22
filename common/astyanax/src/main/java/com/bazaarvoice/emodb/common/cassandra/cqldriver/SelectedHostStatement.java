package com.bazaarvoice.emodb.common.cassandra.cqldriver;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.StatementWrapper;
import com.google.common.annotations.VisibleForTesting;

public class SelectedHostStatement extends StatementWrapper {
    private final Host hostCordinator;
    private final Statement _statement;

    public SelectedHostStatement(Statement wrapped, Host hostCordinator) {
        super(wrapped);
        _statement = wrapped;
        this.hostCordinator = hostCordinator;
    }

    public Host getHostCordinator() {
        return hostCordinator;
    }

    @VisibleForTesting
    public Statement getStatement() {
        return _statement;
    }
}