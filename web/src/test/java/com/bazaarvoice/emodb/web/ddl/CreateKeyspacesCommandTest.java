package com.bazaarvoice.emodb.web.ddl;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;

@Test
public class CreateKeyspacesCommandTest {

    public void testCreateKeyspacesCommand() throws Exception {
        // primarily this test is here to protect in case of relocation of "load.template.cql"
        // on the classpath; and also that expected substitutions in the cql take place

        assertFalse(CqlTemplate.create("/ddl/databus/tables.template.cql")
            .withBinding("keyspace", "databus")
            .toCqlScript().contains("${"));

        assertFalse(CqlTemplate.create("/ddl/queue/tables.template.cql")
                .withBinding("keyspace", "queue")
                .toCqlScript().contains("${"));

        assertFalse(CqlTemplate.create("/ddl/sor/tables.template.cql")
                .withBinding("keyspace", "ugc_global")
                .withBinding("table.audit", "ugc_audit")
                .withBinding("table.delta", "ugc_delta")
                .withBinding("table.history", "ugc_history")
                .toCqlScript().contains("${"));

        assertFalse(CqlTemplate.create("/ddl/media/tables.template.cql")
                .withBinding("keyspace", "media_global")
                .withBinding("table", "ugc_blob")
                .toCqlScript().contains("${"));
    }
}