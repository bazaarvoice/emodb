package com.bazaarvoice.emodb.event.core;

import com.bazaarvoice.emodb.event.api.EventData;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkNotNull;

class DefaultEventData implements EventData {
    private final String _id;
    private final ByteBuffer _data;

    DefaultEventData(String id, ByteBuffer data) {
        _id = checkNotNull(id, "id");
        _data = checkNotNull(data, "data");
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
