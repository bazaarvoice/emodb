package com.bazaarvoice.emodb.databus.api;

import com.google.common.base.CharMatcher;

public abstract class Names {

    /** Prevent instantiation. */
    private Names() {}

    // Exclude whitespace, control chars, non-ascii, upper-case, most punctuation.
    private static final CharMatcher SUBSCRIPTION_NAME_ALLOWED =
            CharMatcher.inRange('a', 'z')
                    .or(CharMatcher.inRange('0', '9'))
                    .or(CharMatcher.anyOf("-.:@_"))
                    .precomputed();

    /**
     * Subscription names must be lowercase ASCII strings. between 1 and 255 characters in length.  Whitespace, ISO
     * control characters and certain punctuation characters that aren't generally allowed in file names or in
     * elasticsearch index names are excluded (elasticsearch appears to allow: !$%&()+-.:;=@[]^_`{}~).  Subscription
     * names may not begin with a single underscore to allow URL space for extensions such as "/_subscription/...".
     * Queue names may not look like relative paths, ie. "." or "..".
     */
    public static boolean isLegalSubscriptionName(String subscription) {
        return subscription != null &&
                subscription.length() > 0 && subscription.length() <= 255 &&
                !(subscription.charAt(0) == '_' && !subscription.startsWith("__")) &&
                !(subscription.charAt(0) == '.' && (".".equals(subscription) || "..".equals(subscription))) &&
                SUBSCRIPTION_NAME_ALLOWED.matchesAllOf(subscription);
    }
}
