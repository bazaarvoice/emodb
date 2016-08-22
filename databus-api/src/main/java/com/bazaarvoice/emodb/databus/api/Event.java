package com.bazaarvoice.emodb.databus.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class Event {

    private final String _eventKey;
    private final Map<String, ?> _content;
    private final List<List<String>> _tags;

    public Event(@JsonProperty("eventKey") String eventKey,
                 @JsonProperty("content") Map<String, ?> content,
                 @JsonProperty("tags") List<List<String>> tags) {
        _eventKey = checkNotNull(eventKey, "eventKey");
        _content = checkNotNull(content, "content");
        // Permit nulls; older version of the API omit tags from the response
        _tags = Objects.firstNonNull(tags, ImmutableList.<List<String>>of());
    }

    @JsonView(EventViews.ContentOnly.class)
    public String getEventKey() {
        return _eventKey;
    }

    @JsonView(EventViews.ContentOnly.class)
    public Map<String, Object> getContent() {
        return Collections.unmodifiableMap(_content);
    }

    @JsonView(EventViews.WithTags.class)
    public List<List<String>> getTags() {
        return _tags;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Event)) {
            return false;
        }
        Event event = (Event) o;
        return _eventKey.equals(event.getEventKey()) &&
                _content.equals(event.getContent()) &&
                _tags.equals(event.getTags());

    }

    @Override
    public int hashCode() {
        return _eventKey.hashCode();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("eventKey", _eventKey)
                .add("content", _content)
                .add("tags", _tags)
                .toString();
    }
}
