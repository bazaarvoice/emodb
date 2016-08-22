package com.bazaarvoice.emodb.blob.api;

import com.google.common.base.CharMatcher;

public abstract class Names {

    /** Prevent instantiation. */
    private Names() {}

    private static final CharMatcher BLOB_ID_ALLOWED = CharMatcher
            .inRange('\u0021', '\u007e')          // excludes whitespace, control chars, non-ascii
            .and(CharMatcher.noneOf("\\/*?\"'<>|,#"))
            .precomputed();

    public static boolean isLegalTableName(String table) {
        return com.bazaarvoice.emodb.common.api.Names.isLegalTableName(table);
    }

    /**
     * Blob IDs must be ASCII strings. between 1 and 255 characters in length.  Whitespace, ISO control
     * characters and certain punctuation characters that aren't generally allowed in file names are excluded.
     */
    public static boolean isLegalBlobId(String blobId) {
        return blobId != null && blobId.length() > 0 && blobId.length() <= 255 &&
                BLOB_ID_ALLOWED.matchesAllOf(blobId);
    }
}
