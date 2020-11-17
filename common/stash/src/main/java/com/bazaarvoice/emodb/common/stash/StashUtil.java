package com.bazaarvoice.emodb.common.stash;

import com.amazonaws.regions.Regions;
import com.fasterxml.jackson.databind.util.ISO8601DateFormat;

import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Utility methods for Stash operations.
 */
public class StashUtil {

    public static final DateTimeFormatter STASH_DIRECTORY_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss").withZone(ZoneOffset.UTC);
    public static final String LATEST_FILE = "_LATEST";
    public static final String SUCCESS_FILE = "_SUCCESS";
    
    /**
     * Converts characters which are valid in table names but not valid or problematic in URLs and S3 keys.
     * Since all EmoDB tables cannot have upper-case characters they make a dense substitution without
     * possibility of collision.
     */
    private static final Map<Character, Character> TABLE_CHAR_REPLACEMENTS = new HashMap<>();
    private static final Map<Character, Character> TABLE_CHAR_REPLACEMENTS_INVERTED = new HashMap<>();
    static {
        TABLE_CHAR_REPLACEMENTS.put(':', '~');
        TABLE_CHAR_REPLACEMENTS.forEach((key, value) -> TABLE_CHAR_REPLACEMENTS_INVERTED.put(value, key));
    }

    // Prevent instantiation
    private StashUtil() {
        // empty
    }

    public static String encodeStashTable(String table) {
        return transformStashTable(table, TABLE_CHAR_REPLACEMENTS);
    }

    public static String decodeStashTable(String table) {
        return transformStashTable(table, TABLE_CHAR_REPLACEMENTS_INVERTED);
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

    public static Optional<String> getRegionForBucket(String bucket) {
        if (bucket == null) {
            return Optional.empty();
        }
        if (bucket.startsWith("emodb-us-east-1")) {
            return Optional.of(Regions.US_EAST_1.getName());
        }
        if (bucket.startsWith("emodb-eu-west-1")) {
            return Optional.of(Regions.EU_WEST_1.getName());
        }
        return Optional.empty();
    }

    public static Date getStashCreationTime(String stashDirectory) {
        Instant stashCreationTime = Instant.from(STASH_DIRECTORY_DATE_FORMAT.parse(stashDirectory));
        return Date.from(stashCreationTime);
    }

    public static Date getStashCreationTimeStamp(String stashStartTime) throws ParseException {
        return new ISO8601DateFormat().parse(stashStartTime);
    }

    public static String getStashDirectoryForCreationTime(Date creationTime) {
        return STASH_DIRECTORY_DATE_FORMAT.format(creationTime.toInstant());
    }
}
