package com.bazaarvoice.emodb.databus.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

public class Event {

    private final String _eventKey;
    private final Map<String, ?> _content;
    private final List<List<String>> _tags;

    public Event(@JsonProperty("eventKey") String eventKey,
                 @JsonProperty("content") Map<String, ?> content,
                 @JsonProperty("tags") List<List<String>> tags) {
        _eventKey = requireNonNull(eventKey, "eventKey");
        _content = requireNonNull(content, "content");
        // Permit nulls; older version of the API omit tags from the response
        _tags = Optional.ofNullable(tags).orElse(Collections.EMPTY_LIST);
    }

    @JsonView(EventViews.ContentOnly.class)
    public String getEventKey() {
        return _eventKey;
    }

    @JsonIgnore
    public Map<String, Object> getContent() {
        return Collections.unmodifiableMap(_content);
    }

    /**
     * For purposes of JSON serialization wrapping the content in an unmodifiable view may cause the serializer
     * to choose a less-optimal implementation.  Since JSON serialization cannot modify the underlying content
     * it is safe to return the original content object to the serializer.
     */
    @JsonView(EventViews.ContentOnly.class)
    @JsonProperty("content")
    private Map<String, Object> getJsonSerializingContent() {
        //noinspection unchecked
        return (Map<String, Object>) _content;
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
        return new StringJoiner(", ", Event.class.getSimpleName() + "[", "]")
                .add("eventKey='" + _eventKey + "'")
                .add("content=" + _content)
                .add("tags=" + _tags)
                .toString();
    }
}
