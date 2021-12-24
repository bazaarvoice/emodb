package com.bazaarvoice.emodb.event.core;

import com.bazaarvoice.emodb.event.api.EventData;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

class DefaultEventData implements EventData {
    private final String _id;
    private final ByteBuffer _data;

    DefaultEventData(String id, ByteBuffer data) {
        _id = requireNonNull(id, "id");
        _data = requireNonNull(data, "data");
    }

    @Override
    public String getId() {
        return _id;
    }

    @Override
    public ByteBuffer getData() {
        return _data.duplicate();
    }
}
