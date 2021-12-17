package com.bazaarvoice.emodb.databus.repl;

import com.bazaarvoice.emodb.databus.core.UpdateRefSerializer;
import com.bazaarvoice.emodb.event.api.EventData;
import com.bazaarvoice.emodb.event.api.EventStore;
import com.bazaarvoice.emodb.sor.core.UpdateRef;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class DefaultReplicationSource implements ReplicationSource {
    private final EventStore _eventStore;

    @Inject
    public DefaultReplicationSource(EventStore eventStore) {
        _eventStore = eventStore;
    }

    @Override
    public List<ReplicationEvent> get(String channel, int limit) {
        requireNonNull(channel, "channel");
        checkArgument(limit > 0, "Limit must be >0");

        List<EventData> rawEvents = _eventStore.peek(channel, limit);

        return Lists.transform(rawEvents, new Function<EventData, ReplicationEvent>() {
            @Override
            public ReplicationEvent apply(EventData rawEvent) {
                UpdateRef ref = UpdateRefSerializer.fromByteBuffer(rawEvent.getData());
                return new ReplicationEvent(rawEvent.getId(), ref);
            }
        });
    }

    @Override
    public void delete(String channel, Collection<String> eventIds) {
        requireNonNull(channel, "channel");
        requireNonNull(eventIds, "eventIds");

        _eventStore.delete(channel, eventIds, false);
    }
}
