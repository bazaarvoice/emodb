package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.event.api.EventData;
import com.bazaarvoice.emodb.event.api.EventStore;

import java.util.Collection;
import java.util.List;

public class EventStoreEventSource implements EventSource {
    private final EventStore _eventStore;
    private final String _channel;

    public EventStoreEventSource(EventStore eventStore, String channel) {
        _eventStore = eventStore;
        _channel = channel;
    }

    @Override
    public List<EventData> get(int limit) {
        return _eventStore.peek(_channel, limit);
    }

    @Override
    public void delete(Collection<String> eventKeys) {
        _eventStore.delete(_channel, eventKeys, false);
    }
}
