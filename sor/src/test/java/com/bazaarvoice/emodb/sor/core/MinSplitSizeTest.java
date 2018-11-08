package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.core.test.InMemoryDataStore;
import com.bazaarvoice.emodb.sor.db.test.InMemoryDataReaderDAO;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.table.db.Table;
import com.codahale.metrics.MetricRegistry;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class MinSplitSizeTest {

    @Test
    public void testMinSplitAfterTimeout() {
        InMemoryDataReaderDAO dataDao = new InMemoryDataReaderDAO() {
            @Override
            public List<String> getSplits(Table table, int desiredRecordsPerSplit, int splitQuerySize) throws TimeoutException {
                if (splitQuerySize <= 10) {
                    throw new TimeoutException();
                }

                return super.getSplits(table, desiredRecordsPerSplit, splitQuerySize);
            }
        };

        DataStore dataStore = new InMemoryDataStore(dataDao, new MetricRegistry());

        dataStore.createTable("table", new TableOptionsBuilder().setPlacement("default").build(),
                Collections.emptyMap(), new AuditBuilder().build());

        for (int i = 0; i < 200; i++) {
            dataStore.update("table", Integer.toString(i), TimeUUIDs.newUUID(), Deltas.fromString("{\"name\":\"Bob\"}"),
                    new AuditBuilder().build(), WriteConsistency.STRONG);
        }

        assertEquals(dataStore.getSplits("table", 50).size(), 4);

        try {
            dataStore.getSplits("table", 10);
            fail();
        } catch (Exception e) {}

        // data store should have cached that 10 is too small from previous request and return splits of 100 instead.
        assertEquals(dataStore.getSplits("table", 10).size(), 20);

    }
}
