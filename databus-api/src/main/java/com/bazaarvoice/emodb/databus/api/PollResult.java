package com.bazaarvoice.emodb.databus.api;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Result returned from {@link Databus#poll(String, Duration, int)}.  The result contains to attributes:
 *
 * <ol>
 *     <li>
 *         The an iterator of {@link Event} instances returned from the poll, returned by {@link #getEventIterator()}.
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

    public PollResult(final Iterator<Event> eventIterator, int approximateSize, boolean moreEvents) {
        requireNonNull(eventIterator, "eventIterator");
        _events = new ArrayList<>(approximateSize);
        _moreEvents = moreEvents;

        // Wrap the event iterator such that each event returned is appended to the event list
        _eventIterator = new Iterator<Event>() {
            @Override
            public boolean hasNext() {
                return eventIterator.hasNext();
            }

            @Override
            public Event next() {
                Event event = eventIterator.next();
                _events.add(event);
                return event;
            }

            @Override
            public void remove() {
                _eventIterator.remove();
            }
        };
    }

    public Iterator<Event> getEventIterator() {
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
