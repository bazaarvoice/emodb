package com.bazaarvoice.emodb.sor.delta.deser;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.uuid.TimeUUIDs;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public class DeltaJsonTest {

    @Test
    public void testDeltaJson() throws IOException {
        // basic literal
        Delta expected = Deltas.literal(3);
        String string = JsonHelper.asJson(expected);
        assertEquals(string, "\"3\"");

        Delta actual = JsonHelper.fromJson(string, Delta.class);
        assertEquals(actual, expected);
    }

    @Test
    public void testTree() throws IOException {
        // reasonably simple hierarchy
        Object json = -3.2e14;
        String key = TimeUUIDs.newUUID().toString();
        Delta expected = Deltas.mapBuilder().
                update("tags", Deltas.mapBuilder().remove("missing").put(key, "EXPERT").build()).
                update("missing", Deltas.conditional(Conditions.equal(json), Deltas.delete())).
                build();
        String string = JsonHelper.asJson(expected);
        assertEquals(string, "\"{..,\\\"missing\\\":if -3.2E14 then ~ end,\\\"tags\\\":{..,\\\"" + key + "\\\":\\\"EXPERT\\\",\\\"missing\\\":~}}\"");

        Delta actual = JsonHelper.fromJson(string, Delta.class);
        assertEquals(actual, expected);
    }

    @Test
    public void testMap() throws IOException {
        // embedded in a Map
        String key = TimeUUIDs.newUUID().toString();
        Map<String, Delta> expected = ImmutableMap.of(
                "uuid1", Deltas.literal(ImmutableMap.of("status", "APPROVED")),
                "uuid2", Deltas.mapBuilder().remove("missing").put(key, "EXPERT").build());
        String string = JsonHelper.asJson(expected);
        assertEquals(string, "{\"uuid1\":\"{\\\"status\\\":\\\"APPROVED\\\"}\",\"uuid2\":\"{..,\\\"" + key + "\\\":\\\"EXPERT\\\",\\\"missing\\\":~}\"}");

        Map<String, Delta> actual = JsonHelper.fromJson(string, new TypeReference<Map<String, Delta>>() {});
        assertEquals(actual, expected);
    }

    @Test
    public void testWrite() {
    }

    @Test
    public void testTestWrite() {
    }

    @Test
    public void testAppend() {
    }

    @Test
    public void testTestAppend() {
    }
}
