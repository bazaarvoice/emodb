package com.bazaarvoice.emodb.web.report;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

public class ReportHistogramTest {

    @Test
    public void testReportHistogram() throws Exception {
        // Run the test 100 times with different numbers
        Random random = new Random(58292517L);

        for (int i=0; i < 100; i++) {
            testReportHistogram(random);
        }
    }

    private void testReportHistogram(Random random) throws Exception {
        // Create 1,000 long values and compute the summary both in total and in batches of varying sizes.

        LongReportHistogram control = new LongReportHistogram();
        LongReportHistogram partial = new LongReportHistogram();

        List<LongReportHistogram> partials = Lists.newArrayList();

        int[] batchSizes = new int[] { 20, 50, 5, 1, 13, 25 };
        int batchIndex = 0;

        for (int i=0; i < 1000; i++) {
            long value = Math.abs(random.nextLong());
            control.update(value);
            partial.update(value);

            if (partial.count() == batchSizes[batchIndex]) {
                // Start a new batch
                partials.add(partial);
                partial = new LongReportHistogram();
                batchIndex = (batchIndex+1) % batchSizes.length;
            }
        }

        if (partial.count() != 0) {
            partials.add(partial);
        }

        LongReportHistogram aggregate = LongReportHistogram.combine(partials);

        assertEquals("aggregate count", control.count(), aggregate.count());
        assertEquals("aggregate min", control.min(), aggregate.min());
        assertEquals("aggregate max", control.max(), aggregate.max());
        assertEquals("aggregate sum", control.sum(), aggregate.sum());
        assertEquals("aggregate mean", control.mean(), aggregate.mean());
        assertClose("aggregate sd", control.stdDev(), aggregate.stdDev());
    }

    public void assertClose(String message, double d1, double d2) {
        // The method for calculating aggregate mean and standard deviation is close but not as exact as
        // computing from the entire data set.
        assertTrue(message, Math.pow(d1 - d2, 2) / Math.pow(d1 + d2, 2) < 0.001);
    }

    @Test
    public void testJsonConversion() throws Exception {
        DateReportHistogram original = new DateReportHistogram();
        Calendar cal = Calendar.getInstance();
        cal.setTime(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss Z").parse("2014/01/15 12:00:00 GMT"));

        for (int i = 0; i < 5; i++) {
            original.update(cal.getTime());
            cal.add(Calendar.DATE, 1);
        }

        ObjectMapper objectMapper = new ObjectMapper().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        String json = objectMapper.writeValueAsString(original);

        DateReportHistogram deser = objectMapper.readValue(json, DateReportHistogram.class);
        assertEquals(original.mean(), deser.mean());
        assertEquals(original.min(), deser.min());
        assertEquals(original.max(), deser.max());
        assertEquals(original.stdDev(), deser.stdDev());
    }
}
