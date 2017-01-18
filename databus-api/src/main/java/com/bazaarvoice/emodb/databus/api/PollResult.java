package com.bazaarvoice.emodb.databus.api;

import org.joda.time.Duration;

import java.util.List;

/**
 * Result returned from {@link Databus#poll(String, Duration, int)}.  The result contains to attributes:
 *
 * <ol>
 *     <li>
 *         The list of {@link Event} instances returned from the poll.
 *     </li>
 *     <li>
 *         A boolean indicator for whether there are more events and the caller would benefit from immediately re-polling
 *         versus delaying for a brief period before polling again.  Because the databus discards redundant events it's
 *         possible that it will return less than the requested number of events even though the queue is not empty.
 *         In an extreme example if the queue is deep and contains nothing but redundant events then the poll result may
 *         contain no events yet still indicate that there are more events.
 *     </li>
 * </ol>
 */
public class PollResult {

    private final List<Event> _events;
    private final boolean _moreEvents;

    public PollResult(List<Event> events, boolean moreEvents) {
        _events = events;
        _moreEvents = moreEvents;
    }

    public List<Event> getEvents() {
        return _events;
    }

    public boolean hasMoreEvents() {
        return _moreEvents;
    }
}
