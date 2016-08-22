package com.bazaarvoice.emodb.databus;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ChannelNamesTest {
    @Test
    public void testNonDeduped() {
        assertTrue(ChannelNames.isNonDeduped("__system_bus:master"));
        assertTrue(ChannelNames.isNonDeduped("__system_bus:replay"));
        assertTrue(ChannelNames.isNonDeduped("__system_bus:out:eu-west-1"));
        assertTrue(ChannelNames.isNonDeduped("__dedupq_read:my-queue"));   // internal databus read channel.
        assertTrue(ChannelNames.isNonDeduped("__dedupq_write:my-queue"));  // this isn't a name used by the databus.

        assertFalse(ChannelNames.isNonDeduped("my-queue"));
        assertFalse(ChannelNames.isNonDeduped("__system_bus:canary"));
        assertFalse(ChannelNames.isNonDeduped("__system_bus:canary-ci_sor_cat_default"));
    }
}
