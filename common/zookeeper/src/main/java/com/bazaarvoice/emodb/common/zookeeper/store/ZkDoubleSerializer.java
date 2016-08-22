package com.bazaarvoice.emodb.common.zookeeper.store;

public class ZkDoubleSerializer implements ZkValueSerializer<Double> {
    @Override
    public String toString(Double value) {
        return value.toString();
    }

    @Override
    public Double fromString(String string) {
        return Double.parseDouble(string);
    }
}
