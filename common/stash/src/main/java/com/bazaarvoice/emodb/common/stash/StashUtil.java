package com.bazaarvoice.emodb.common.stash;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.bazaarvoice.emodb.common.json.OrderedJson;
import com.bazaarvoice.emodb.common.json.RisonHelper;
import com.fasterxml.jackson.databind.util.ISO8601DateFormat;
import com.google.common.base.CharMatcher;
import com.google.common.base.Charsets;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.text.ParseException;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

/**
 * Utility methods for Stash operations.
 */
public class StashUtil {

    public static final DateTimeFormatter STASH_DIRECTORY_DATE_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm-ss").withZoneUTC();
    public static final String LATEST_FILE = "_LATEST";
    public static final String SUCCESS_FILE = "_SUCCESS";

    private static final Map<String, Region> REGION_BY_BUCKET = ImmutableMap.of(
            "emodb-us-east-1", Region.getRegion(Regions.US_EAST_1),
            "emodb-eu-west-1", Region.getRegion(Regions.EU_WEST_1));

    /**
     * Converts characters which are valid in table names but not valid or problematic in URLs and S3 keys.
     * Since all EmoDB tables cannot have upper-case characters they make a dense substitution without
     * possibility of collision.
     */
    private static final BiMap<Character, Character> TABLE_CHAR_REPLACEMENTS =
            ImmutableBiMap.of(':', '~');

    /**
     * S3 object tag keys and values can only be letters, numbers, whitespace, or one of the characters in the
     * final matcher below.   Technically '/' is also permitted but we'll reserve that as an escape character for
     * other characters, so chosen because it's not part of a valid table name and is arbitrarily one of the
     * remaining options: '/', '+', or '='.
     */
    private static final CharMatcher S3_VALID_TAG_CHARACTERS =
            CharMatcher.inRange('a', 'z').
                    or(CharMatcher.inRange('A', 'Z')).
                    or(CharMatcher.inRange('0', '9')).
                    or(CharMatcher.anyOf(" +-=._:")).
                    precomputed();

    private static final String S3_TABLE_OBJECT_TAG = asSafeS3ObjectTagKey("emodb:intrinsic:table");
    private static final String S3_PLACEMENT_OBJECT_TAG = asSafeS3ObjectTagKey("emodb:intrinsic:placement");

    // Prevent instantiation
    private StashUtil() {
        // empty
    }

    public static String encodeStashTable(String table) {
        return transformStashTable(table, TABLE_CHAR_REPLACEMENTS);
    }

    public static String decodeStashTable(String table) {
        return transformStashTable(table, TABLE_CHAR_REPLACEMENTS.inverse());
    }

    private static String transformStashTable(String table, Map<Character, Character> transformCharMap) {
        if (table == null) {
            return null;
        }

        for (Map.Entry<Character, Character> entry : transformCharMap.entrySet()) {
            table = table.replace(entry.getKey(), entry.getValue());
        }

        return table;
    }

    public static Region getRegionForBucket(String bucket) {
        Region region = REGION_BY_BUCKET.get(bucket);
        if (region == null) {
            // Default to us-east-1 if unknown
            region = Region.getRegion(Regions.US_EAST_1);
        }
        return region;
    }

    public static Date getStashCreationTime(String stashDirectory) {
        return STASH_DIRECTORY_DATE_FORMAT.parseDateTime(stashDirectory).toDate();
    }

    public static Date getStashCreationTimeStamp(String stashStartTime) throws ParseException {
        return new ISO8601DateFormat().parse(stashStartTime);
    }

    public static String getStashDirectoryForCreationTime(Date creationTime) {
        return STASH_DIRECTORY_DATE_FORMAT.print(creationTime.getTime());
    }

    public static String getTableTagKey() {
        return S3_TABLE_OBJECT_TAG;
    }

    public static String getPlacementTagKey() {
        return S3_PLACEMENT_OBJECT_TAG;
    }

    public static String getTemplateTagKey(String templateAttribute) {
        return asSafeS3ObjectTagKey("emodb:template:" + templateAttribute);
    }

    /**
     * S3 object tag keys have a maximum length of 128 characters.  If the key is longer than this it is replaced with
     * an MD5 hash of the original value.
     */
    private static String asSafeS3ObjectTagKey(String key) {
        return asSafeS3ObjectTagComponent(key, 128);
    }

    /**
     * Converts a Java representation of a JSON value to an S3 tag value.  For Maps and Lists the value is the Rison
     * representation of the object.  Rison is more condensed than JSON and better suited for use in AWS policies.
     * For all other data types it is the natural representation.  This allows for some collision in possible values
     * since, for example, the number one and the string "1" would both have the same value.  However, strings are the
     * most common table metadata value type and forcing a JSON representation complete with double-quotes and escape
     * sequences is cumbersome when creating AWS policies.  Additionally, if the organization uses a consistent
     * vocabulary then metadata values such as "type" and "client" would consistently be of the same data type
     * across tables.  Therefore, favor usability over absolute value separation.
     *
     * S3 object tag values have a maximum length of 256 characters.  If the value is longer than this it is replaced with
     * an MD5 hash of the original value.
     */
    public static String getTagValue(Object value) {
        String tagValue;
        if (value instanceof Map || value instanceof Collection) {
            // If the value is a map or contains any maps sort them by key to produce a consistent JSON string.
            Object orderedValue = OrderedJson.ordered(value);
            tagValue = RisonHelper.asORison(orderedValue);
        } else {
            tagValue = value != null ? value.toString() : "null";
        }
        return asSafeS3ObjectTagComponent(tagValue, 256);
    }

    private static String asSafeS3ObjectTagComponent(String value, int maxLength) {
        // Most commonly the value only contains valid tag characters, so only copy to a new String if necessary.
        StringBuilder safeValueBuilder = null;

        for (int i=0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (!S3_VALID_TAG_CHARACTERS.matches(c)) {
                if (safeValueBuilder == null) {
                    safeValueBuilder = new StringBuilder(value.substring(0, i));
                }
                // Write a hex representation of the character.  Most common encodings use characters which aren't
                // valid in S3, such as "%xx" or "\\uxxxx", so we'll use our own within those limited constraints,
                // "/xxxx".
                safeValueBuilder.append("/").append(String.format("%04x", (int) c));
            } else if (safeValueBuilder != null) {
                safeValueBuilder.append(c);
            }
        }

        String safeValue = safeValueBuilder != null ? safeValueBuilder.toString() : value;

        if (safeValue.length() <= maxLength) {
            return safeValue;
        }

        // Since we can't write the safe value substitute it with an MD5 hash of the original value.  Not perfect, but
        // consistent and deterministic.
        String md5 = Hashing.md5().hashString(value, Charsets.UTF_8).toString();
        return "emodb:md5:" + md5;
    }
}
