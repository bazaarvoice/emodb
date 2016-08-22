package com.bazaarvoice.emodb.common.json;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Recursively converts all maps within a JSON object to sorted LinkedHashMaps so that the toString()
 * representations are deterministic.  Useful for debugging and for tests that validate string output.
 */
public abstract class OrderedJson {

    public static final Ordering<String> KEY_COMPARATOR = new Ordering<String>() {
        @Override
        public int compare(String key1, String key2) {
            return ComparisonChain.start()
                    .compareTrueFirst(key1.startsWith("~"), key2.startsWith("~"))
                    .compare(key1, key2)
                    .result();
        }
    };

    public static final Ordering<Map.Entry<String, ?>> ENTRY_COMPARATOR = new Ordering<Map.Entry<String, ?>>() {
        @Override
        public int compare(Map.Entry<String, ?> left, Map.Entry<String, ?> right) {
            return KEY_COMPARATOR.compare(left.getKey(), right.getKey());
        }
    };

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
        List<Object> sortedList = Lists.newArrayListWithCapacity(list.size());
        for (Object value : list) {
            sortedList.add(ordered(value));
        }
        return sortedList;
    }

    private static Map<String, Object> ordered(Map<String, ?> map) {
        List<? extends Map.Entry<String, ?>> entries = ENTRY_COMPARATOR.immutableSortedCopy(map.entrySet());

        // use a LinkedHashMap instead of ImmutableMap because JSON may contain null values
        Map<String, Object> sortedMap = Maps.newLinkedHashMap();
        for (Map.Entry<String, ?> entry : entries) {
            sortedMap.put(entry.getKey(), ordered(entry.getValue()));
        }
        return sortedMap;
    }

    public static List<String> orderedStrings(Collection<?> col) {
        List<String> sortedStrings = Lists.newArrayListWithCapacity(col.size());
        for (Object value : col) {
            sortedStrings.add(JsonHelper.asJson(ordered(value)));
        }
        Collections.sort(sortedStrings);
        return sortedStrings;
    }
}
