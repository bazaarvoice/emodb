package com.bazaarvoice.emodb.databus.api;

import java.util.BitSet;

import static com.bazaarvoice.emodb.common.api.Names.anyCharInRange;
import static com.bazaarvoice.emodb.common.api.Names.anyCharInString;
import static com.bazaarvoice.emodb.common.api.Names.anyOf;

public abstract class Names {

    /** Prevent instantiation. */
    private Names() {}

    // Exclude whitespace, control chars, non-ascii, upper-case, most punctuation.
    private static final BitSet SUBSCRIPTION_NAME_ALLOWED = anyOf(
            anyCharInRange('a', 'z'),
            anyCharInRange('0', '9'),
            anyCharInString("-.:@_"));

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
                subscription.chars().allMatch(SUBSCRIPTION_NAME_ALLOWED::get);
    }
}
