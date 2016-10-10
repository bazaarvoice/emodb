package com.bazaarvoice.emodb.sor.db.cql;

import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.sor.db.DataReaderDAO;
import com.bazaarvoice.emodb.sor.db.astyanax.CqlDataReaderDAO;
import com.bazaarvoice.emodb.table.db.astyanax.PlacementCache;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMultimap;
import org.testng.annotations.Test;

import java.io.PrintWriter;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

public class SorCqlSettingsTaskTest {
    @Test
    public void testTask()
            throws Exception {
        DataReaderDAO astyanaxDelegate = mock(DataReaderDAO.class);
        DataReaderDAO dataReaderDAO = new CqlDataReaderDAO(astyanaxDelegate, mock(PlacementCache.class),
                mock(MetricRegistry.class));
        int defaultFetchSize = ((CqlDataReaderDAO) dataReaderDAO).getSingleRowFetchSize();
        SorCqlSettingsTask task =
                new SorCqlSettingsTask(mock(TaskRegistry.class), dataReaderDAO, Suppliers.ofInstance(true),
                        Suppliers.ofInstance(true));

        // Verify the fetch size is the same as default of 10
        assertEquals(((CqlDataReaderDAO) dataReaderDAO).getSingleRowFetchSize(), defaultFetchSize, "Fetch size should be the default.");

        // Try modifying fetch size and prefetch limit
        int expectedFetchSize = 15;
        int expectedPrefetchLimit = 5;
        task.execute(ImmutableMultimap.<String, String>builder()
                .put("fetchSize", Integer.toString(expectedFetchSize))
                .put("prefetchLimit", Integer.toString(expectedPrefetchLimit))
                .build(), new PrintWriter(System.out));
        // Verify the fetch size is changed to 15 and the prefetch limit is changed to 5
        assertEquals(((CqlDataReaderDAO) dataReaderDAO).getSingleRowFetchSize(), expectedFetchSize, "Fetch size should be changed.");
        assertEquals(((CqlDataReaderDAO) dataReaderDAO).getSingleRowPrefetchLimit(), expectedPrefetchLimit, "Prefetch limit should be changed.");
    }
}
