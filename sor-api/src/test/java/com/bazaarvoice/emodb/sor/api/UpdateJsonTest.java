package com.bazaarvoice.emodb.sor.api;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.uuid.TimeUUIDs;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.UUID;

import static org.testng.Assert.assertEquals;

public class UpdateJsonTest {

    /** Round trip an {@link Update} object to and from JSON. */
    @Test
    public void testUpdateJson() throws IOException {
        UUID uuid = TimeUUIDs.newUUID();
        Update expected = new Update("my-table", "Key", uuid, Deltas.delete(), new AuditBuilder().setHost("test").build(), WriteConsistency.STRONG);

        String string = JsonHelper.asJson(expected);
        assertEquals(string, "{\"table\":\"my-table\",\"key\":\"Key\",\"changeId\":\"" + uuid + "\",\"delta\":\"~\",\"audit\":{\"host\":\"test\"},\"consistency\":\"STRONG\"}");

        Update actual = JsonHelper.fromJson(string, Update.class);
        assertEquals(actual.getTable(), expected.getTable());
        assertEquals(actual.getKey(), expected.getKey());
        assertEquals(actual.getChangeId(), expected.getChangeId());
        assertEquals(actual.getDelta(), expected.getDelta());
        assertEquals(actual.getAudit().getAll(), expected.getAudit().getAll());
        assertEquals(actual.getConsistency(), expected.getConsistency());
    }
}
