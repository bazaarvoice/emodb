package com.bazaarvoice.emodb.databus.api;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class EventJsonTest {

    @Test
    public void testEventJson() throws Exception {
        Event expected = new Event("key",
                ImmutableMap.<String, Object>of("name", "Bob", "state", "APPROVED"),
                ImmutableList.<List<String>>of(ImmutableList.of("tag1", "tag2")));

        String eventString = JsonHelper.asJson(expected);
        assertEquals(eventString, "{\"eventKey\":\"key\",\"content\":{\"name\":\"Bob\",\"state\":\"APPROVED\"},\"tags\":[[\"tag1\",\"tag2\"]]}");

        Event actual = JsonHelper.fromJson(eventString, Event.class);
        assertEquals(actual.getEventKey(), expected.getEventKey());
        assertEquals(actual.getContent(), expected.getContent());
        assertEquals(actual.getTags(), expected.getTags());
    }

    @Test
    public void testEventJsonViews() throws Exception {
        Event original = new Event("key",
                ImmutableMap.<String, Object>of("name", "Bob", "state", "APPROVED"),
                ImmutableList.<List<String>>of(ImmutableList.of("tag1", "tag2")));

        Event expectedContentOnly = new Event("key",
                ImmutableMap.<String, Object>of("name", "Bob", "state", "APPROVED"),
                ImmutableList.<List<String>>of());

        String actualContentOnlyString = JsonHelper.withView(EventViews.ContentOnly.class).asJson(original);
        String actualWithTagsString = JsonHelper.withView(EventViews.WithTags.class).asJson(original);

        Event actualContentOnly = JsonHelper.fromJson(actualContentOnlyString, Event.class);
        Event actualWithTags = JsonHelper.fromJson(actualWithTagsString, Event.class);

        assertEquals(actualContentOnly, expectedContentOnly);
        assertEquals(actualWithTags, original);

        // The above tests verify serialization and deserialization worked, but independently verify that the
        // "content only" version didn't have a tags attribute in the JSON string at all.
        assertFalse(actualContentOnlyString.contains("\"tags\""));
        assertTrue(actualWithTagsString.contains("\"tags\""));
    }
}
