package com.bazaarvoice.emodb.blob.api;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.sor.api.TableAvailability;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertEquals;

public class TableJsonTest {

    @Test
    public void testTableJson() throws IOException {
        TableOptions options = new TableOptionsBuilder().setPlacement("aPlacement").build();
        TableAvailability availability = new TableAvailability("aPlacement", false);
        Table expected = new DefaultTable("aName", options, ImmutableMap.<String, String>of("key", "value"), availability);

        String string = JsonHelper.asJson(expected);
        assertEquals(string, "{\"name\":\"aName\",\"options\":{\"placement\":\"aPlacement\",\"facades\":[]},\"attributes\":{\"key\":\"value\"},\"availability\":{\"placement\":\"aPlacement\",\"facade\":false}}");

        Table actual = JsonHelper.fromJson(string, Table.class);
        assertEquals(actual, expected);
        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getOptions(), expected.getOptions());
        assertEquals(actual.getAttributes(), expected.getAttributes());
        assertEquals(actual.hashCode(), expected.hashCode());
    }
}
