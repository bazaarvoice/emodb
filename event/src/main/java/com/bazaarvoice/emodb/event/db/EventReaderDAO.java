package com.bazaarvoice.emodb.event.db;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.Iterator;

public interface EventReaderDAO {

    Iterator<String> listChannels();

    long count(String channel, long limit);

    /**
     * Read all events for the channel and pass then to the sink until the sink returns false.
     * If 'since' is non-null, then most of the events before 'since' time will be skipped.
     * Some prior to 'since' time (at most 999) may still get through, but all events on or after since are guaranteed.
     * */
    void readAll(String channel, EventSink sink, @Nullable Date since);

    /**
     * Read events for the channel and pass then to the sink until the sink returns false.  If called repeatedly
     * over time (a minute or so) this is guaranteed to eventually return all events for the channel.  But most of the
     * time this method will limit its search to events written recently.
     */
    void readNewer(String channel, EventSink sink);

    /**
     * Moves <em>some</em> events from one channel to another when the events can be moved in bulk without reading
     * individual event data.  At best this will move all the events, at worst it will move zero events.
     * @return true if all events were definitely moved, false if some events might not have been moved.
     */
    boolean moveIfFast(String fromChannel, String toChannel);
}
