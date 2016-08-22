package com.bazaarvoice.emodb.databus.repl;

import java.util.Collection;
import java.util.List;

/**
 * Fetch/acknowledge interface for consuming a stream of events to be replicated.
 */
public interface ReplicationSource {

    List<ReplicationEvent> get(String channel, int limit);

    void delete(String channel, Collection<String> eventIds);
}
