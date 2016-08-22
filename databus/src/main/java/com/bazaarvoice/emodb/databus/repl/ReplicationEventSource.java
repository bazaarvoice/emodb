package com.bazaarvoice.emodb.databus.repl;

import com.bazaarvoice.emodb.databus.core.EventSource;
import com.bazaarvoice.emodb.databus.core.UpdateRefSerializer;
import com.bazaarvoice.emodb.event.api.EventData;
import com.bazaarvoice.emodb.sor.core.UpdateRef;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

/**
 * Adapts an instance of {@link ReplicationSource} to the {@link com.bazaarvoice.emodb.databus.core.EventSource} interface.  This is used
 * by a replication sink to fetch events originating from a remote data center and fan them out to
 * subscriptions in the local data center.
 */
public class ReplicationEventSource implements EventSource {
    private final ReplicationSource _source;
    private final String _channel;

    public ReplicationEventSource(ReplicationSource source, String channel) {
        _source = source;
        _channel = channel;
    }

    @Override
    public List<EventData> get(int limit) {
        List<ReplicationEvent> events = _source.get(_channel, limit);

        return Lists.transform(events, new Function<ReplicationEvent, EventData>() {
            @Override
            public EventData apply(final ReplicationEvent event) {
                return new EventData() {
                    @Override
                    public String getId() {
                        return event.getId();
                    }

                    @Override
                    public ByteBuffer getData() {
                        return UpdateRefSerializer.toByteBuffer(
                                new UpdateRef(event.getTable(), event.getKey(), event.getChangeId(), event.getTags()));
                    }
                };
            }
        });
    }

    @Override
    public void delete(Collection<String> eventIds) {
        _source.delete(_channel, eventIds);
    }
}
