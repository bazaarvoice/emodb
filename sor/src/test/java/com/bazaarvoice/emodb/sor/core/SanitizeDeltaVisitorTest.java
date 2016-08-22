package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.delta.MapDeltaBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class SanitizeDeltaVisitorTest {

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNull() {
        SanitizeDeltaVisitor.sanitize(Deltas.literal(null));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testFalse() {
        SanitizeDeltaVisitor.sanitize(Deltas.literal(false));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTrue() {
        SanitizeDeltaVisitor.sanitize(Deltas.literal(true));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNumber() {
        SanitizeDeltaVisitor.sanitize(Deltas.literal(1));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testString() {
        SanitizeDeltaVisitor.sanitize(Deltas.literal("hello world"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testList() {
        SanitizeDeltaVisitor.sanitize(Deltas.literal(ImmutableList.of(1, 2)));
    }

    @Test
    public void testMap() {
        SanitizeDeltaVisitor.sanitize(Deltas.literal(ImmutableMap.of("x", 1)));
    }

    @Test
    public void testDelete() {
        Delta delta = Deltas.delete();
        Delta actual = SanitizeDeltaVisitor.sanitize(delta);
        assertEquals(actual, delta);
    }

    @Test
    public void testNoop() {
        Delta delta = Deltas.noop();
        Delta actual = SanitizeDeltaVisitor.sanitize(delta);
        assertEquals(actual, delta);
    }

    @Test
    public void testMapDelta() {
        MapDeltaBuilder builder = Deltas.mapBuilder();
        builder.put("~x", 1);
        for (String field : Intrinsic.DATA_FIELDS) {
            builder.put(field, 1);
        }
        builder.remove("~y");
        Delta delta = builder.build();

        Delta actual = SanitizeDeltaVisitor.sanitize(delta);

        assertEquals(actual, Deltas.mapBuilder().put("~x", 1).remove("~y").build());
    }

    @Test
    public void testSanitizeTagsAttribute() {
        Delta actual = SanitizeDeltaVisitor.sanitize(
                Deltas.fromString("{..,\"name\":\"Bob\", \"" +
                        UpdateRef.TAGS_NAME +
                        "\":[\"tag0\"]}"));
        assertEquals(actual, Deltas.fromString("{..,\"name\":\"Bob\"}"));
    }

    @Test
    public void testConditionalDelta() {
        MapDeltaBuilder thenBuilder = Deltas.mapBuilder();
        thenBuilder.put("~x", 1);
        for (String field : Intrinsic.DATA_FIELDS) {
            thenBuilder.put(field, 1);
        }
        thenBuilder.remove("~y");
        Delta thenClause = thenBuilder.build();

        MapDeltaBuilder elseBuilder = Deltas.mapBuilder();
        elseBuilder.remove("~x");
        for (String field : Intrinsic.DATA_FIELDS) {
            elseBuilder.put(field, 2);
        }
        elseBuilder.put("~y", 2);
        Delta elseClause = elseBuilder.build();

        Delta actual = SanitizeDeltaVisitor.sanitize(Deltas.conditional(Conditions.isDefined(), thenClause, elseClause));

        assertEquals(actual, Deltas.conditional(Conditions.isDefined(),
                Deltas.mapBuilder().put("~x", 1).remove("~y").build(),
                Deltas.mapBuilder().remove("~x").put("~y", 2).build()));
    }
}
