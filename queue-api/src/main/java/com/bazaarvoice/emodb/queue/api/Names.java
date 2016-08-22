package com.bazaarvoice.emodb.queue.api;

import com.google.common.base.CharMatcher;

public abstract class Names {

    /** Prevent instantiation. */
    private Names() {}

    // Exclude whitespace, control chars, non-ascii, upper-case, most punctuation.
    private static final CharMatcher QUEUE_NAME_ALLOWED =
            CharMatcher.inRange('a', 'z')
                    .or(CharMatcher.inRange('0', '9'))
                    .or(CharMatcher.anyOf("-.:@_"))
                    .precomputed();

    /**
     * Queue names must be lowercase ASCII strings. between 1 and 255 characters in length.  Whitespace, ISO control
     * characters and most punctuation characters aren't allowed.  Queue names may not begin with a single underscore
     * to allow URL space for extensions such as "/_queue/...".  Queue names may not look like relative paths, ie.
     * "." or "..".
     */
    public static boolean isLegalQueueName(String queue) {
        return queue != null &&
                queue.length() > 0 && queue.length() <= 255 &&
                !(queue.charAt(0) == '_' && !queue.startsWith("__")) &&
                !(queue.charAt(0) == '.' && (".".equals(queue) || "..".equals(queue))) &&
                QUEUE_NAME_ALLOWED.matchesAllOf(queue);
    }
}
