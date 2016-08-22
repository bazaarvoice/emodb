package com.bazaarvoice.emodb.event.api;

import com.google.common.collect.Lists;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class SimpleEventSink implements EventSink {
    private final List<EventData> _events = Lists.newArrayList();
    private int _remaining;

    public SimpleEventSink(int limit) {
        checkArgument(limit > 0, "Limit must be >0");
        _remaining = limit;
    }

    @Override
    public int remaining() {
        return _remaining;
    }

    @Override
    public Status accept(EventData event) {
        _events.add(event);
        return --_remaining > 0 ? Status.ACCEPTED_CONTINUE : Status.ACCEPTED_STOP;
    }

    public List<EventData> getEvents() {
        return _events;
    }
}
