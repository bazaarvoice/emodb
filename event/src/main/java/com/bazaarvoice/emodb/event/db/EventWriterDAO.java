package com.bazaarvoice.emodb.event.db;

import com.google.common.collect.Multimap;

import java.nio.ByteBuffer;
import java.util.Collection;

public interface EventWriterDAO {

    void addAll(Multimap<String, ByteBuffer> eventsByChannel, EventSink sink);

    void delete(String channel, Collection<EventId> eventIds);

    void deleteAll(String channel);
}
