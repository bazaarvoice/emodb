package com.bazaarvoice.emodb.queue.api;

import com.google.common.base.Strings;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class NamesTest {
    @Test
    public void testLegality() {
        assertIllegal("");

        assertIllegal("_");
        assertLegal("__");

        // '.' and '..' are not legal zookeeper path names so disallow them in subscription names too.
        assertIllegal(".");
        assertIllegal("..");
        assertLegal("...");

        assertLegal("-");
        assertLegal(":");
        assertLegal("@");

        assertLegal("0");
        assertLegal("9");

        assertLegal("a");
        assertLegal("z");
        assertIllegal("A");
        assertIllegal("Z");

        // non-ascii
        assertIllegal("\u221a\u2022");

        // selection of punctuation and whitespace and other similar characters
        assertIllegal("#");
        assertIllegal("&");
        assertIllegal("^");
        assertIllegal("%");
        assertIllegal(" ");
        assertIllegal("\t");
        assertIllegal("\n");
        assertIllegal("\u0000");

        assertLegal(Strings.repeat("x", 255));
        assertIllegal(Strings.repeat("x", 256));
    }

    private void assertLegal(String name) {
        assertTrue(Names.isLegalQueueName(name), name);
    }

    private void assertIllegal(String name) {
        assertFalse(Names.isLegalQueueName(name), name);
    }
}
