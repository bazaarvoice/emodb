package com.bazaarvoice.emodb.databus.api;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.joda.time.Duration;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Result returned from {@link Databus#poll(String, Duration, int)}.  The result contains to attributes:
 *
 * <ol>
 *     <li>
 *         The an iterator of {@link Event} instances returned from the poll, returned by {@link #getEventStream()}.
 *     </li>
 *     <li>
 *         A boolean indicator for whether there are more events and the caller would benefit from immediately re-polling
 *         versus delaying for a brief period before polling again.  Because the databus discards redundant events it's
 *         possible that it will return less than the requested number of events even though the queue is not empty.
 *         In an extreme example if the queue is deep and contains nothing but redundant events then the poll result may
 *         contain no events yet still indicate that there are more events.
 *     </li>
 * </ol>
 *
 * As a convenience the result has a {@link #getEvents()} method which returns all of the events from the stream in a
 * list.  This is present primarily to support older non-streaming implementations and may be removed in a future
 * release.
 */
public class PollResult {

    private final Iterator<Event> _eventIterator;
    private final List<Event> _events;
    private final boolean _moreEvents;

    public PollResult(Iterator<Event> eventIterator, int approximateSize, boolean moreEvents) {
        checkNotNull(eventIterator, "eventIterator");
        _events = Lists.newArrayListWithCapacity(approximateSize);
        _moreEvents = moreEvents;

        // Wrap the event iterator such that each event returned is appended to the event list
        _eventIterator = Iterators.transform(eventIterator, event -> {
            _events.add(event);
            return event;
        });
    }

    public Iterator<Event> getEventStream() {
        return _eventIterator;
    }

    @Deprecated
    public List<Event> getEvents() {
        // Iterate over the entire iterator to ensure the event list is completely filled
        while (_eventIterator.hasNext()) {
            _eventIterator.next();
        }
        return _events;
    }

    public boolean hasMoreEvents() {
        return _moreEvents;
    }
}
