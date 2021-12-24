package com.bazaarvoice.emodb.sor.api;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.google.common.collect.ImmutableSet;

import java.util.Date;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public abstract class Intrinsic {
    // Data Fields

    public static final String ID = "~id";
    public static final String TABLE = "~table";
    public static final String VERSION = "~version";
    public static final String SIGNATURE = "~signature";
    public static final String DELETED = "~deleted";
    public static final String FIRST_UPDATE_AT = "~firstUpdateAt";
    public static final String LAST_UPDATE_AT = "~lastUpdateAt";
    public static final String LAST_MUTATE_AT = "~lastMutateAt";
    public static final String PLACEMENT = "~placement";

    public static final Set<String> DATA_FIELDS =
            ImmutableSet.of(ID, TABLE, VERSION, SIGNATURE, DELETED, FIRST_UPDATE_AT, LAST_UPDATE_AT,
                    LAST_MUTATE_AT, PLACEMENT);

    // Audit Fields

    public static final String AUDIT_SHA1 = Audit.SHA1;

    // Utility methods

    /** Prevent instantiation. */
    private Intrinsic() {}

    public static String getId(Map<String, ?> content) {
        return (String) requireNonNull(content.get(ID), ID);
    }

    public static String getTable(Map<String, ?> content) {
        return (String) requireNonNull(content.get(TABLE), TABLE);
    }

    public static Long getVersion(Map<String, ?> content) {
        Object version = content.get(VERSION);  // optional, will be null when reads use weak consistency
        return (version instanceof Long) ? (Long) version : (version != null) ? ((Number) version).longValue() : null;
    }

    public static String getSignature(Map<String, ?> content) {
        return (String) requireNonNull(content.get(SIGNATURE), SIGNATURE);
    }

    public static boolean isDeleted(Map<String, ?> content) {
        return (Boolean) requireNonNull(content.get(DELETED), DELETED);
    }

    public static Date getFirstUpdateAt(Map<String, ?> content) {
        return JsonHelper.parseTimestamp((String) content.get(FIRST_UPDATE_AT));
    }

    public static Date getLastUpdateAt(Map<String, ?> content) {
        return JsonHelper.parseTimestamp((String) content.get(LAST_UPDATE_AT));
    }

    public static Date getLastMutateAt(Map<String, ?> content) {
        return JsonHelper.parseTimestamp((String) content.get(LAST_MUTATE_AT));
    }

}
