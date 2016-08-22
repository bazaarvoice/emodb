package com.bazaarvoice.emodb.queue.api;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class MessageJsonTest {

    @Test
    public void testMessageJson() throws Exception {
        Message expected = new Message("messageId", ImmutableMap.<String, Object>of("name", "Bob", "state", "APPROVED"));

        String messageString = JsonHelper.asJson(expected);
        assertEquals(messageString, "{\"id\":\"messageId\",\"payload\":{\"name\":\"Bob\",\"state\":\"APPROVED\"}}");

        Message actual = JsonHelper.fromJson(messageString, Message.class);
        assertEquals(actual.getId(), expected.getId());
        assertEquals(actual.getPayload(), expected.getPayload());
    }
}
