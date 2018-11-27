package com.bazaarvoice.emodb.blob.api;

import java.util.BitSet;

import static com.bazaarvoice.emodb.common.api.Names.anyCharExcept;
import static com.bazaarvoice.emodb.common.api.Names.anyCharInRange;
import static com.bazaarvoice.emodb.common.api.Names.anyCharInString;

public abstract class Names {

    /** Prevent instantiation. */
    private Names() {}

    private static final BitSet BLOB_ID_ALLOWED = anyCharExcept(
            anyCharInRange('\u0021', '\u007e'),          // excludes whitespace, control chars, non-ascii
            anyCharInString("\\/*?\"'<>|,#"));

    public static boolean isLegalTableName(String table) {
        return com.bazaarvoice.emodb.common.api.Names.isLegalTableName(table);
    }

    /**
     * Blob IDs must be ASCII strings. between 1 and 255 characters in length.  Whitespace, ISO control
     * characters and certain punctuation characters that aren't generally allowed in file names are excluded.
     */
    public static boolean isLegalBlobId(String blobId) {
        return blobId != null && blobId.length() > 0 && blobId.length() <= 255 &&
                blobId.chars().allMatch(BLOB_ID_ALLOWED::get);
    }
}
