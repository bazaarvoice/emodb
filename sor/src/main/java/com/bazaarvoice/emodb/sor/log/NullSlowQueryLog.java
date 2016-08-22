package com.bazaarvoice.emodb.sor.log;

import com.bazaarvoice.emodb.sor.core.Expanded;

public class NullSlowQueryLog implements SlowQueryLog {

    @Override
    public void log(String table, String key, Expanded record) {
        // Do nothing
    }
}
