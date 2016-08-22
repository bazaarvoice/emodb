package com.bazaarvoice.emodb.sor.log;

import com.bazaarvoice.emodb.sor.core.Expanded;

public interface SlowQueryLog {

    void log(String table, String key, Expanded record);
}
