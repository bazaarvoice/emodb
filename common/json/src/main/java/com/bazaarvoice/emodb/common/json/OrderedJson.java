package com.bazaarvoice.emodb.common.json;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Recursively converts all maps within a JSON object to sorted LinkedHashMaps so that the toString()
 * representations are deterministic.  Useful for debugging and for tests that validate string output.
 */
//TODO add tests
public abstract class OrderedJson {

    public static final Comparator<String> KEY_COMPARATOR = (key1, key2) -> {
        if (key1.startsWith("~")) {
            if (!key2.startsWith("~")) {
                return -1;
            }
        } else if (key2.startsWith("~")) {
            return 1;
        }
        return key1.compareTo(key2);
    };

    public static final Comparator<Map.Entry<String, ?>> ENTRY_COMPARATOR = (left, right) -> KEY_COMPARATOR.compare(left.getKey(), right.getKey());

    public static Object ordered(@Nullable Object obj) {
        if (obj instanceof Map) {
            //noinspection unchecked
            return ordered((Map<String, ?>) obj);
        } else if (obj instanceof List) {
            return ordered((List<?>) obj);
        } else {
            return obj;
        }
    }

    private static List<Object> ordered(List<?> list) {
        // don't sort the list because that would change intent.  only maps and sets get sorted.
        List<Object> sortedList = new ArrayList<>(list.size());
        for (Object value : list) {
            sortedList.add(ordered(value));
        }
        return sortedList;
    }

    private static Map<String, Object> ordered(Map<String, ?> map) {
        Map<String, Object> sortedMap = new LinkedHashMap<>();
        map.entrySet().stream().sorted(ENTRY_COMPARATOR).forEach(e -> sortedMap.put(e.getKey(), ordered(e.getValue())));
        return sortedMap;
    }

    public static List<String> orderedStrings(Collection<?> col) {
        List<String> sortedStrings = new ArrayList<>(col.size());
        for (Object value : col) {
            sortedStrings.add(JsonHelper.asJson(ordered(value)));
        }
        Collections.sort(sortedStrings);
        return sortedStrings;
    }
}
