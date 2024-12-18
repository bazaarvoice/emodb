package com.bazaarvoice.emodb.sor.api;

public abstract class Names {

    /** Prevent instantiation. */
    private Names() {}

    /**
     * Table names must be lowercase ASCII strings. between 1 and 255 characters in length.  Whitespace, ISO control
     * characters and certain punctuation characters that aren't generally allowed in file names or in elasticsearch
     * index names are excluded (elasticsearch appears to allow: !$%&()+-.:;=@[]^_`{}~).  Table names may not begin
     * with a single underscore to allow URL space for extensions such as "/_table/...".
     */
    public static boolean isLegalTableName(String table) {
        return com.bazaarvoice.emodb.common.api.Names.isLegalTableName(table);
    }

    public static boolean isLegalTableAttributeName(String attributeName) {
        // The attributes should not start with "~" which is reserved for Emodb's internal use
        return !attributeName.startsWith("~");
    }


}
