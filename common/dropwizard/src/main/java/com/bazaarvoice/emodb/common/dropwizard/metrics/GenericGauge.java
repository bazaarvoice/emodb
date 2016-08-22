package com.bazaarvoice.emodb.common.dropwizard.metrics;

import com.codahale.metrics.Gauge;

public class GenericGauge implements Gauge<Object> {
    private volatile Object _value;

    @Override
    public Object getValue() {
        return _value;
    }

    public void set(Object value) {
        _value = value;
    }
}
