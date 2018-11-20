package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.core.test.InMemoryDataStore;
import com.bazaarvoice.emodb.sor.db.astyanax.AstyanaxDataReaderDAO;
import com.bazaarvoice.emodb.sor.db.astyanax.ChangeEncoder;
import com.bazaarvoice.emodb.sor.db.test.InMemoryDataReaderDAO;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.astyanax.PlacementCache;
import com.codahale.metrics.MetricRegistry;
import com.netflix.astyanax.partitioner.BOP20Partitioner;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.dht.Token;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;
import static org.mockito.Mockito.mock;


public class MinSplitSizeTest {

    @Test
    public void testMinSplitAfterTimeout() {
        InMemoryDataReaderDAO dataDao = new InMemoryDataReaderDAO() {
            @Override
            public List<String> getSplits(Table table, int recordsPerSplit, int localResplits) throws TimeoutException {

                if (recordsPerSplit <= 10) {
                    throw new TimeoutException();
                }

                return super.getSplits(table, recordsPerSplit, localResplits);
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

        // Splits should come back normally
        assertEquals(dataStore.getSplits("table", 10).size(), 20);

    }

    @Test
    public void testLocalResplitting() {
        AstyanaxDataReaderDAO astyanaxDataReaderDAO = new AstyanaxDataReaderDAO(mock(PlacementCache.class), mock(ChangeEncoder.class), new MetricRegistry());

        String[] minMaxResplits3Times = {
                "0000000000000000000000000000000000000000",
                "1fffffffffffffffffffffffffffffffffffffffe0",
                "3fffffffffffffffffffffffffffffffffffffffc0",
                "5fffffffffffffffffffffffffffffffffffffffa0",
                "7fffffffffffffffffffffffffffffffffffffff80",
                "9fffffffffffffffffffffffffffffffffffffff60",
                "bfffffffffffffffffffffffffffffffffffffff40",
                "dfffffffffffffffffffffffffffffffffffffff20",
                "ffffffffffffffffffffffffffffffffffffffff"
        };

        List<Token> splits = astyanaxDataReaderDAO.resplitLocally(BOP20Partitioner.MINIMUM, BOP20Partitioner.MAXIMUM, 3);

        assertEquals(splits.size(), minMaxResplits3Times.length);

        for (int i =0; i < splits.size(); i++) {
            assertEquals(splits.get(i).toString(), minMaxResplits3Times[i]);
        }

        // Number of tokens should be 2^10 + 1 = 1025, as 1025 consecutive tokens can form 1024 ranges.
        assertEquals(astyanaxDataReaderDAO.resplitLocally(BOP20Partitioner.MINIMUM, BOP20Partitioner.MAXIMUM, 10).size(), 1025);

        // Number of tokens should be 2^5 + 1 = 33, as 33 consecutive tokens can form 32 ranges.
        assertEquals(astyanaxDataReaderDAO.resplitLocally(BOP20Partitioner.MINIMUM, BOP20Partitioner.MAXIMUM, 5).size(), 33);

        List<Token> unsplitTokens = astyanaxDataReaderDAO.resplitLocally(BOP20Partitioner.MINIMUM, BOP20Partitioner.MAXIMUM, 0);

        // Number of tokens should be 2^0 + 1 = 2, as 2 consecutive tokens can form 1 range.
        assertEquals(unsplitTokens.size(), 2);
        assertEquals(unsplitTokens.get(0).toString(), BOP20Partitioner.MINIMUM);
        assertEquals(unsplitTokens.get(1).toString(), BOP20Partitioner.MAXIMUM);

    }
}
