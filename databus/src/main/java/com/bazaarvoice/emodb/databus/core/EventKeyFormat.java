package com.bazaarvoice.emodb.databus.core;

import com.google.common.base.CharMatcher;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/** Multiple EventStore event IDs can be combined into a single Databus event key when they refer to the same object. */
class EventKeyFormat {
    /** Event IDs are lowercase hex.  Enforce this so we know they don't conflict with the separator characters. */
    private static final CharMatcher VALID_EVENT_ID =
            CharMatcher.inRange('0', '9').or(CharMatcher.inRange('a', 'f')).precomputed();

    /**
     * Ends an event ID where the next event ID starts from scratch, does not share a common prefix with the next.
     * Must be a non-lowercase hex character.
     */
    private static final char DELIM_REGULAR = 'I';

    /**
     * Ends an event ID where the next event ID is the same length and shares a common prefix with the previous.
     * The text that follows is the suffix of the following event ID that differs from the previous.  For example,
     * the event IDs "abcdef", "abcghi" are encoded as "abcdefXghi".  This works well with EventStore event IDs
     * which are typically "[32-char-slab-id][4-char-counter][4-char-checksum]".  Subsequent event IDs usually share
     * the same slab ID, the 4-char counter increases sequentially, and the 4-char checksum varies.
     */
    private static final char DELIM_SHARED_PREFIX = 'X';

    /**
     * Combine multiple EventStore event IDs into a single Databus event key.  To get the most compact encoded string,
     * sort the event ID list before encoding it.
     */
    static String encode(List<String> eventIds) {
        checkArgument(!eventIds.isEmpty(), "Empty event ID list.");
        if (eventIds.size() == 1) {
            return checkValid(eventIds.get(0));
        }
        // Concatenate the event IDs using a simple scheme that efficiently encodes events that are the same length and
        // share a common prefix.
        StringBuilder buf = new StringBuilder();
        String prevId = null;
        for (String eventId : eventIds) {
            checkValid(eventId);
            int commonPrefixLength;
            if (prevId == null) {
                // First event ID
                buf.append(eventId);
            } else if (prevId.length() == eventId.length() &&
                    (commonPrefixLength = getCommonPrefixLength(prevId, eventId)) > 0) {
                // Event ID is same length and shares a common prefix with the previous.  Just add the part that's different.
                buf.append(DELIM_SHARED_PREFIX).append(eventId.substring(commonPrefixLength));
            } else {
                buf.append(DELIM_REGULAR).append(eventId);
            }
            prevId = eventId;
        }
        return buf.toString();
    }

    /** Split Databus event keys into EventStore event IDs. */
    static List<String> decodeAll(Collection<String> eventKeys) {
        List<String> eventIds = Lists.newArrayList();
        for (String eventKey : eventKeys) {
            decodeTo(eventKey, eventIds);
        }
        return eventIds;
    }

    /** Split a Databus event key into EventStore event IDs. */
    static void decodeTo(String eventKey, Collection<String> eventIds) {
        int startIdx = 0;
        String prevId = null;
        for (int i = 0; i < eventKey.length(); i++) {
            char ch = eventKey.charAt(i);
            if (ch == DELIM_REGULAR || ch == DELIM_SHARED_PREFIX) {
                String eventId = checkValid(combine(prevId, eventKey.substring(startIdx, i)));
                eventIds.add(eventId);
                prevId = (ch == DELIM_REGULAR) ? null : eventId;
                startIdx = i + 1;
            }
        }
        // Add the final event ID
        eventIds.add(checkValid(combine(prevId, eventKey.substring(startIdx, eventKey.length()))));
    }

    private static int getCommonPrefixLength(String s1, String s2) {
        int max = Math.min(s1.length(), s2.length());
        for (int i = 0; i < max; i++) {
            if (s1.charAt(i) != s2.charAt(i)) {
                return i;
            }
        }
        return max;
    }

    private static String combine(@Nullable String prevId, String string) {
        if (prevId == null) {
            return string;
        } else {
            // Replace the last string.length() chars of "prevId" with "string"
            return prevId.substring(0, prevId.length() - string.length()) + string;
        }
    }

    private static String checkValid(String eventId) {
        checkArgument(!eventId.isEmpty() && VALID_EVENT_ID.matchesAllOf(eventId), "Invalid event ID: %s", eventId);
        return eventId;
    }
}
