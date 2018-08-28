package com.bazaarvoice.emodb.sor.api;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import static org.testng.Assert.assertEquals;

/**
 * Serialize and deserialize an {@link Change} object using the Jackson JSON parser.
 */
public class ChangeJsonTest {

    private static ObjectMapper _mapper = new ObjectMapper();

    @Test
    public void testChangeJson() throws IOException {
        UUID changeId = TimeUUIDs.newUUID();
        Delta delta = Deltas.literal(ImmutableMap.of("name", "Bob", "state", "SUBMITTED"));
        History history = new History(changeId,
                ImmutableMap.<String, Object>of("name", "Bob", "state", "SUBMITTED"), delta);
        Change expected = new ChangeBuilder(changeId).with(delta).with(history).build();

        SimpleDateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZZ");
        dateFmt.setTimeZone(TimeZone.getTimeZone("UTC"));

        String string = JsonHelper.asJson(expected);
        assertEquals(string, "{" +
                "\"timestamp\":\"" + dateFmt.format(TimeUUIDs.getDate(changeId)) + "\"," +
                "\"id\":\"" + changeId + "\"," +
                "\"delta\":\"{\\\"name\\\":\\\"Bob\\\",\\\"state\\\":\\\"SUBMITTED\\\"}\"," +
                "\"history\":{\"changeId\":\"" + changeId + "\",\"delta\":\"{\\\"name\\\":\\\"Bob\\\",\\\"state\\\":\\\"SUBMITTED\\\"}\",\"content\":{\"name\":\"Bob\",\"state\":\"SUBMITTED\"}}," +
                "\"tags\":[]}");

        Change actual = JsonHelper.fromJson(string, Change.class);
        assertEquals(actual.getDelta(), delta);
        assertEquals(toMap(actual), toMap(expected));
    }

    @Test
    public void testLegacyCompactionJson() throws IOException {
        UUID changeId = TimeUUIDs.newUUID();
        Delta delta = Deltas.literal(ImmutableMap.of("name", "Bob", "state", "SUBMITTED"));
        UUID first = TimeUUIDs.newUUID();
        UUID cutoff = TimeUUIDs.newUUID();
        String cutoffSignature = "00000000000000000000000000000000";
        Compaction compaction = new Compaction(3, first, cutoff, cutoffSignature, cutoff, cutoff);
        Change expected = new ChangeBuilder(changeId).with(delta).with(compaction).build();

        SimpleDateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZZ");
        dateFmt.setTimeZone(TimeZone.getTimeZone("UTC"));

        String string = JsonHelper.asJson(expected);
        assertEquals(string, "{" +
                "\"timestamp\":\"" + dateFmt.format(TimeUUIDs.getDate(changeId)) + "\"," +
                "\"id\":\"" + changeId + "\"," +
                "\"delta\":\"{\\\"name\\\":\\\"Bob\\\",\\\"state\\\":\\\"SUBMITTED\\\"}\"," +
                "\"compaction\":{\"count\":3,\"first\":\"" + first + "\",\"cutoff\":\"" + cutoff + "\",\"cutoffSignature\":\"00000000000000000000000000000000\",\"lastContentMutation\":\"" + cutoff + "\",\"lastMutation\":\"" + cutoff + "\",\"lastTags\":[]}," +
                "\"tags\":[]}");

        Change actual = JsonHelper.fromJson(string, Change.class);
        assertEquals(actual.getDelta(), delta);
        assertEquals(toMap(actual), toMap(expected));
    }

    @Test
    public void testCompactionJson() throws IOException {
        UUID changeId = TimeUUIDs.newUUID();
        Delta delta = Deltas.literal(ImmutableMap.of("name", "Bob", "state", "SUBMITTED"));
        UUID first = TimeUUIDs.newUUID();
        UUID cutoff = TimeUUIDs.newUUID();
        String cutoffSignature = "00000000000000000000000000000000";
        Compaction compaction = new Compaction(3, first, cutoff, cutoffSignature, cutoff, cutoff, delta);
        Change expected = new ChangeBuilder(changeId).with(delta).with(compaction).build();

        SimpleDateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZZ");
        dateFmt.setTimeZone(TimeZone.getTimeZone("UTC"));

        String string = JsonHelper.asJson(expected);
        assertEquals(string, "{" +
                "\"timestamp\":\"" + dateFmt.format(TimeUUIDs.getDate(changeId)) + "\"," +
                "\"id\":\"" + changeId + "\"," +
                "\"delta\":\"{\\\"name\\\":\\\"Bob\\\",\\\"state\\\":\\\"SUBMITTED\\\"}\"," +
                "\"compaction\":{\"count\":3,\"first\":\"" + first + "\",\"cutoff\":\"" + cutoff + "\",\"cutoffSignature\":\"00000000000000000000000000000000\",\"lastContentMutation\":\"" + cutoff + "\",\"lastMutation\":\"" + cutoff + "\",\"compactedDelta\":\"{\\\"name\\\":\\\"Bob\\\",\\\"state\\\":\\\"SUBMITTED\\\"}\",\"lastTags\":[]}," +
                "\"tags\":[]}");

        Change actual = JsonHelper.fromJson(string, Change.class);
        assertEquals(actual.getDelta(), delta);
        assertEquals(toMap(actual), toMap(expected));
    }

    private Map<String, Object> toMap(Change change) {
        return _mapper.convertValue(change, Map.class);
    }
}
