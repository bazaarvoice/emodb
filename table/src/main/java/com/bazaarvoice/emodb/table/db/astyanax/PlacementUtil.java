package com.bazaarvoice.emodb.table.db.astyanax;

import static java.lang.String.format;

public class PlacementUtil {
    public static String[] parsePlacement(String placement) {
        int colon = placement.indexOf(':');
        if (colon == -1) {
            throw new IllegalArgumentException(format(
                    "Placement string must be in the format '<keyspace>:<family>': %s", placement));
        }
        String keyspaceName = placement.substring(0, colon);
        String columnFamilyPrefix = placement.substring(colon + 1);
        return new String[]{ keyspaceName, columnFamilyPrefix };
    }
}
