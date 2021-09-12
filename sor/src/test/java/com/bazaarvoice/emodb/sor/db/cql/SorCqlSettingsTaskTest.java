package com.bazaarvoice.emodb.sor.db.cql;

import com.bazaarvoice.emodb.common.cassandra.CqlDriverConfiguration;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
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
        CqlDriverConfiguration cqlDriverConfig = new CqlDriverConfiguration();
        int defaultFetchSize = cqlDriverConfig.getSingleRowFetchSize();
        SorCqlSettingsTask task =
                new SorCqlSettingsTask(mock(TaskRegistry.class), cqlDriverConfig, Suppliers.ofInstance(true),
                        Suppliers.ofInstance(true));

        // Verify the fetch size is the same as default of 10
        assertEquals(cqlDriverConfig.getSingleRowFetchSize(), defaultFetchSize, "Fetch size should be the default.");

        // Try modifying fetch size and prefetch limit
        int expectedFetchSize = 15;
        int expectedPrefetchLimit = 5;
        task.execute(ImmutableMultimap.<String, String>builder()
                .put("fetchSize", Integer.toString(expectedFetchSize))
                .put("prefetchLimit", Integer.toString(expectedPrefetchLimit))
                .build(), new PrintWriter(System.out));
        // Verify the fetch size is changed to 15 and the prefetch limit is changed to 5
        assertEquals(cqlDriverConfig.getSingleRowFetchSize(), expectedFetchSize, "Fetch size should be changed.");
        assertEquals(cqlDriverConfig.getSingleRowPrefetchLimit(), expectedPrefetchLimit, "Prefetch limit should be changed.");
    }
}
