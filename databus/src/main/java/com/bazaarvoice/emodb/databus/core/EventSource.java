package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.event.api.EventData;

import java.util.Collection;
import java.util.List;

public interface EventSource {
    List<EventData> get(int limit);

    void delete(Collection<String> eventKeys);
}
