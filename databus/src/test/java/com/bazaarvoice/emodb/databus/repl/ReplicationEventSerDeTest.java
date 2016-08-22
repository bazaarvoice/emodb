package com.bazaarvoice.emodb.databus.repl;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ReplicationEventSerDeTest {
    @Test
    public void testRoundTrip() {

        // Legacy ReplicationEvent
        String legacyReplicationEventJson = "{\"id\":\"some_id\",\"table\":\"some_table\",\"key\":\"some_key\",\"changeId\":\"" + TimeUUIDs.newUUID().toString() + "\"}";
        ReplicationEvent re = JsonHelper.fromJson(legacyReplicationEventJson, ReplicationEvent.class);
        assertTrue(re.getTags().isEmpty());

        String replicationEventJson = "{\"id\":\"some_id\",\"table\":\"some_table\",\"key\":\"some_key\",\"changeId\":\"" + TimeUUIDs.newUUID().toString() + "\"," +
                "\"tags\":[\"ignore\"]}";
        re = JsonHelper.fromJson(replicationEventJson, ReplicationEvent.class);
        assertEquals(re.getTags(), ImmutableSet.of("ignore"));
    }
}
