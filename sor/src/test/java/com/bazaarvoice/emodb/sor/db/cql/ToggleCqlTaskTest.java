package com.bazaarvoice.emodb.sor.db.cql;

import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.db.DataReaderDAO;
import com.bazaarvoice.emodb.sor.db.Key;
import com.bazaarvoice.emodb.sor.db.astyanax.CqlDataReaderDAO;
import com.bazaarvoice.emodb.table.db.astyanax.PlacementCache;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.io.PrintWriter;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class ToggleCqlTaskTest {
    @Test
    public void testToggleTask()
            throws Exception {
        DataReaderDAO astyanaxDelegate = mock(DataReaderDAO.class);
        DataReaderDAO dataReaderDAO = new CqlDataReaderDAO(astyanaxDelegate, mock(PlacementCache.class),
                mock(MetricRegistry.class));
        int defaultFetchSize = ((CqlDataReaderDAO) dataReaderDAO).getSingleRowFetchSize();
        ToggleCqlAstyanaxTask toggleCqlTask =
                new ToggleCqlAstyanaxTask(mock(TaskRegistry.class), dataReaderDAO);

        // Verify that by default Cql is used for single and multi-gets
        assertFalse(((CqlDataReaderDAO)dataReaderDAO).getDelegateToAstyanax(), "By default it should use CQL when it can");

        toggleCqlTask.execute(ImmutableMultimap.<String, String>builder().put("driver","cql").build(), new PrintWriter(System.out));

        // Verify the calls do not make it to astyanax
        try {
            dataReaderDAO.read(mock(Key.class), ReadConsistency.STRONG);
            fail();
        } catch (NullPointerException e) {
            // Ignore the NPE that we will get due to mock classes being used
        }

        try {
            dataReaderDAO.readAll(Lists.newArrayList(mock(Key.class)), ReadConsistency.STRONG);
            fail();
        } catch (NullPointerException e) {
            // Ignore the NPE that we will get due to mock classes being used
        }
        verifyNoMoreInteractions(astyanaxDelegate);

        // Let's toggle to bypass CQL and go directly to Astyanax
        toggleCqlTask.execute(ImmutableMultimap.<String, String>builder().put("driver","astyanax").build(), new PrintWriter(System.out));
        assertTrue(((CqlDataReaderDAO) dataReaderDAO).getDelegateToAstyanax(), "It should be toggled to delegate all data reader dao to astyanax");
        // Verify the fetch size is the same as default of 10
        assertEquals(((CqlDataReaderDAO) dataReaderDAO).getSingleRowFetchSize(), defaultFetchSize, "Fetch size should be the default.");

        dataReaderDAO.read(mock(Key.class), ReadConsistency.STRONG);
        dataReaderDAO.readAll(Lists.newArrayList(mock(Key.class)), ReadConsistency.STRONG);
        verify(astyanaxDelegate, times(1)).read(any(Key.class), any(ReadConsistency.class));
        verify(astyanaxDelegate, times(1)).readAll(any(List.class), any(ReadConsistency.class));

        // Try modifying fetch size and prefetch limit
        int expectedFetchSize = 15;
        int expectedPrefetchLimit = 5;
        toggleCqlTask.execute(ImmutableMultimap.<String, String>builder()
                .put("fetchSize", Integer.toString(expectedFetchSize))
                .put("prefetchLimit", Integer.toString(expectedPrefetchLimit))
                .build(), new PrintWriter(System.out));
        // Verify the fetch size is changed to 15 and the prefetch limit is changed to 5
        assertEquals(((CqlDataReaderDAO) dataReaderDAO).getSingleRowFetchSize(), expectedFetchSize, "Fetch size should be changed.");
        assertEquals(((CqlDataReaderDAO) dataReaderDAO).getSingleRowPrefetchLimit(), expectedPrefetchLimit, "Prefetch limit should be changed.");
    }
}
