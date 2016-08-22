package com.bazaarvoice.emodb.sor.api;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertEquals;

public class TableJsonTest {

    @Test
    public void testTableJson() throws IOException {
        TableOptions options = new TableOptionsBuilder().setPlacement("aPlacement").build();
        TableAvailability availability = new TableAvailability("aPlacement", false);
        Table expected = new DefaultTable("aName", options, ImmutableMap.<String, Object>of("key", "value"), availability);

        String string = JsonHelper.asJson(expected);
        assertEquals(string, "{\"name\":\"aName\",\"options\":{\"placement\":\"aPlacement\",\"facades\":[]},\"template\":{\"key\":\"value\"},\"availability\":{\"placement\":\"aPlacement\",\"facade\":false}}");

        Table actual = JsonHelper.fromJson(string, Table.class);
        assertEquals(actual, expected);
        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getOptions(), expected.getOptions());
        assertEquals(actual.getTemplate(), expected.getTemplate());
        assertEquals(actual.hashCode(), expected.hashCode());
    }
}
