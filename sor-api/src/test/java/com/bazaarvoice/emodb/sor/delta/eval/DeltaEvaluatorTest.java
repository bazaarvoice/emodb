package com.bazaarvoice.emodb.sor.delta.eval;

import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class DeltaEvaluatorTest {

    @Test
    public void testSetList() {
        Object root = DeltaEvaluator.UNDEFINED;
        Delta delta;

        // replace the tags list with ["NEWBIE"]
        delta = Deltas.mapBuilder().
                put("tags", Arrays.asList("NEWBIE")).
                build();
        root = eval(delta, root);
        assertEquals(root, ImmutableMap.of("tags", Arrays.asList("NEWBIE")));
    }

    @Test
    public void testTopLevelDelete() {
        Object root = DeltaEvaluator.UNDEFINED;
        Delta delta;

        // set the initial state of the object
        delta = Deltas.mapBuilder().
                put("name", "Bob").
                removeRest().
                build();
        root = eval(delta, root);
        assertEquals(root, ImmutableMap.of("name", "Bob"));

        // delete the object
        delta = Deltas.delete();
        root = eval(delta, root);
        assertEquals(root, DeltaEvaluator.UNDEFINED);

        // apply an update that, presumably, occurred because it raced the delete and lost
        delta = Deltas.mapBuilder().
                put("state", "APPROVED").
                build();
        root = eval(delta, root);
        assertEquals(root, ImmutableMap.of("state", "APPROVED"));
    }

    @Test
    public void testMidLevelDelete() {
        Object root = DeltaEvaluator.UNDEFINED;
        Delta delta;

        // set the initial state of the object
        delta = Deltas.mapBuilder().
                put("name", "Bob").
                put("avatar", ImmutableMap.of("url", "http://images.example.com/1234")).
                removeRest().
                build();
        root = eval(delta, root);
        assertEquals(root, ImmutableMap.of("name", "Bob", "avatar", ImmutableMap.of("url", "http://images.example.com/1234")));

        // delete the avatar
        delta = Deltas.mapBuilder().
                remove("avatar").
                build();
        root = eval(delta, root);
        assertEquals(root, ImmutableMap.of("name", "Bob"));

        // apply an update that, presumably, occurred because it raced the delete and lost
        delta = Deltas.mapBuilder().
                updateIfExists("avatar", Deltas.mapBuilder().put("state", "APPROVED").build()).
                build();
        root = eval(delta, root);
        assertEquals(root, ImmutableMap.of("name", "Bob"));

        // but the delete doesn't last forever.  a new update can undo the previous delete
        delta = Deltas.mapBuilder().
                put("avatar", ImmutableMap.of("url", "http://images.example.com/2345")).
                build();
        root = eval(delta, root);
        assertEquals(root, ImmutableMap.of("name", "Bob", "avatar", ImmutableMap.of("url", "http://images.example.com/2345")));
    }

    @Test
    public void testRetain() {
        Object root = ImmutableMap.of("name", "Bob", "version", 5, "private", ImmutableMap.of("uid", "bob", "uxid", "123xyz"));
        Delta delta = Deltas.mapBuilder().
                update("private", Deltas.mapBuilder().put("uxid", "789abc").build()).
                retain("version").
                removeRest().
                build();
        root = eval(delta, root);
        assertEquals(root, ImmutableMap.of("version", 5, "private", ImmutableMap.of("uid", "bob", "uxid", "789abc")));
    }

    @Test
    public void testMapUpdate() {
        Delta delta;

        // The combination of a conditional update and removeRest() means a MapDelta can't be constant
        delta = Deltas.mapBuilder().updateIfExists("key", Deltas.literal(5)).removeRest().build();
        assertEquals(eval(delta, ImmutableMap.of("xyz", 1, "key", 2)), ImmutableMap.of("key", 5));
        assertEquals(eval(delta, ImmutableMap.of("xyz", 1)), ImmutableMap.of());
        assertFalse(delta.isConstant());

        // However, the same test case using addOrUpdate() instead is constant.
        delta = Deltas.mapBuilder().update("key", Deltas.literal(5)).removeRest().build();
        assertEquals(eval(delta, ImmutableMap.of("xyz", 1, "key", 2)), ImmutableMap.of("key", 5));
        assertEquals(eval(delta, ImmutableMap.of("xyz", 1)), ImmutableMap.of("key", 5));
        assertTrue(delta.isConstant());
    }

    @Test
    public void testMapDeleteIfEmpty() {
        Delta delta;

        delta = Deltas.mapBuilder().build();
        assertEquals(eval(delta, DeltaEvaluator.UNDEFINED), ImmutableMap.of());

        delta = Deltas.mapBuilder().deleteIfEmpty().build();
        assertEquals(eval(delta, ImmutableMap.of()), DeltaEvaluator.UNDEFINED);

        delta = Deltas.mapBuilder().updateIfExists("y", Deltas.literal(3)).deleteIfEmpty().build();
        assertEquals(eval(delta, DeltaEvaluator.UNDEFINED), DeltaEvaluator.UNDEFINED);

        delta = Deltas.mapBuilder().putIfAbsent("y", 3).deleteIfEmpty().build();
        assertEquals(eval(delta, DeltaEvaluator.UNDEFINED), ImmutableMap.of("y", 3));
    }

    @Test
    public void testNewSet() {
        Object root = DeltaEvaluator.UNDEFINED;
        Delta delta = Deltas.mapBuilder()
                .update("sizes", Deltas.setBuilder().addAll("high", "med", "low").build())
                .build();

        root = eval(delta, root);
        assertEquals(root, ImmutableMap.of("sizes", ImmutableList.of("high", "low", "med")));
    }

    @Test
    public void testModifySet() {
        Object root = ImmutableMap.of("stooges", ImmutableList.of("Larry", "Curly", "Moe"));

        Delta delta = Deltas.mapBuilder()
                .update("stooges", Deltas.setBuilder().remove("Larry").build())
                .build();
        assertEquals(eval(delta, root), ImmutableMap.of("stooges", ImmutableList.of("Curly", "Moe")));

        delta = Deltas.mapBuilder()
                .update("stooges", Deltas.setBuilder().add("Shemp").build())
                .build();
        assertEquals(eval(delta, root), ImmutableMap.of("stooges", ImmutableList.of("Curly", "Larry", "Moe", "Shemp")));

        delta = Deltas.mapBuilder()
                .update("stooges", Deltas.setBuilder().remove("Curly").add("Shemp").build())
                .build();
        assertEquals(eval(delta, root), ImmutableMap.of("stooges", ImmutableList.of("Larry", "Moe", "Shemp")));

        delta = Deltas.mapBuilder()
                .update("stooges", Deltas.setBuilder().removeAll("Larry", "Curly", "Moe").build())
                .build();
        assertEquals(eval(delta, root), ImmutableMap.of("stooges", ImmutableList.of()));

        delta = Deltas.mapBuilder()
                .update("stooges", Deltas.setBuilder().add(null).remove("Moe").build())
                .build();
        assertEquals(eval(delta, root), ImmutableMap.of("stooges", Arrays.asList(null, "Curly", "Larry")));
    }

    @Test
    public void testSetDeleteIfEmpty() {
        Delta delta = Deltas.setBuilder().build();
        assertEquals(eval(delta, DeltaEvaluator.UNDEFINED), ImmutableList.of());

        delta = Deltas.setBuilder().deleteIfEmpty().build();
        assertEquals(eval(delta, ImmutableList.of()), DeltaEvaluator.UNDEFINED);

        delta = Deltas.setBuilder().remove(5).deleteIfEmpty().build();
        assertEquals(eval(delta, DeltaEvaluator.UNDEFINED), DeltaEvaluator.UNDEFINED);

        delta = Deltas.setBuilder().remove(5).deleteIfEmpty().build();
        assertEquals(eval(delta, ImmutableList.of(5)), DeltaEvaluator.UNDEFINED);

        delta = Deltas.setBuilder().add(5).deleteIfEmpty().build();
        assertEquals(eval(delta, DeltaEvaluator.UNDEFINED), ImmutableList.of(5));
    }

    private Object eval(Delta delta, Object root) {
        return DeltaEvaluator.eval(delta, root, Mockito.mock(Intrinsics.class));
    }
}
