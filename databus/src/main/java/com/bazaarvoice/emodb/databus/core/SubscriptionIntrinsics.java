package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.TableFilterIntrinsics;

/**
 * Intrinsics for evaluation during fanout to subscribers.  Only intrinsics available during fanout -- table, placement,
 * and id -- are supported.
 */
public class SubscriptionIntrinsics extends TableFilterIntrinsics {

    private final String _key;

    public SubscriptionIntrinsics(Table table, String key) {
        super(table);
        _key = key;
    }

    @Override
    public String getId() {
        return _key;
    }
}
