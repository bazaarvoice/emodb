package com.bazaarvoice.emodb.queue.api;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class BaseQueueServiceTest {
    /**
     * Because of an Ostrich bug the BaseQueueService methods have been copied to QueueService.java.
     * Verify that this copy has been done correctly and the 3 interfaces are identical.
     */
    @Test
    public void testQueueApisMatch() {
        MapDifference<List<Object>, Method> diff = Maps.difference(
                getDeclaredPublicMethodMap(BaseQueueService.class),
                getDeclaredPublicMethodMap(QueueService.class));

        assertTrue(diff.entriesOnlyOnLeft().isEmpty(), "In BaseQueueService but not in QueueService: " + diff.entriesOnlyOnLeft().values());
        assertTrue(diff.entriesOnlyOnRight().isEmpty(), "In QueueService but not in BaseQueueService: " + diff.entriesOnlyOnRight().values());
    }

    /**
     * Because of an Ostrich bug the BaseQueueService methods have been copied to DedupQueueService.java.
     * Verify that this copy has been done correctly and the 3 interfaces are identical.
     */
    @Test
    public void testDedupQueueApisMatch() {
        MapDifference<List<Object>, Method> diff = Maps.difference(
                getDeclaredPublicMethodMap(BaseQueueService.class),
                getDeclaredPublicMethodMap(DedupQueueService.class));

        assertTrue(diff.entriesOnlyOnLeft().isEmpty(), "In BaseQueueService but not in DedupQueueService: " + diff.entriesOnlyOnLeft().values());
        assertTrue(diff.entriesOnlyOnRight().isEmpty(), "In DedupQueueService but not in BaseQueueService: " + diff.entriesOnlyOnRight().values());
    }

    private Map<List<Object>, Method> getDeclaredPublicMethodMap(Class clazz) {
        Map<List<Object>, Method> map = Maps.newHashMap();
        for (Method method : clazz.getDeclaredMethods()) {
            if ((method.getModifiers() & Modifier.PUBLIC) == 0) {
                continue;
            }

            // Compare: name, arg types, return type.  This is similar to Method.equals() except it ignores the declaring class.
            List<Object> signature = ImmutableList.<Object>of(
                    method.getName(), ImmutableList.copyOf(method.getParameterTypes()), method.getReturnType());

            Method previous = map.put(signature, method);
            assertNull(previous, method + " collides with " + previous);
        }
        return map;
    }
}