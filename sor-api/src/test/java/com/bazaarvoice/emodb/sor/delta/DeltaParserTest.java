package com.bazaarvoice.emodb.sor.delta;

import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.condition.State;
import com.bazaarvoice.emodb.sor.uuid.TimeUUIDs;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class DeltaParserTest {

    @Test
    public void testSimpleMap() {
        // basic test of adding something to a map
        doTest(
                "{..,\"tags\":[\"EARLY_ADOPTER\"]}",
                "{..,\"tags\":[\"EARLY_ADOPTER\"]}",
                Deltas.mapBuilder().put("tags", Arrays.asList("EARLY_ADOPTER")));
    }

    @Test
    public void testComplicatedMap() {
        // more complicated with deletes, conditional update, negative exponential numbers
        Object json = -3.2e14;
        String key = TimeUUIDs.newUUID().toString();
        doTest(
                "{..,\"tags\":{..,\"missing\":~,\"" + key + "\":\"EXPERT\"},\"missing\":if -3.2e14 then ~ end}",
                "{..,\"missing\":if -3.2E14 then ~ end,\"tags\":{..,\"" + key + "\":\"EXPERT\",\"missing\":~}}",
                Deltas.mapBuilder().
                        update("tags", Deltas.mapBuilder().remove("missing").put(key, "EXPERT").build()).
                        update("missing", Deltas.conditional(Conditions.equal(json), Deltas.delete())));
    }

    @Test
    public void testUnicodeEscape() {
        // quoting of key values with special characters
        doTest(
                "{..,\"a\\\"b\\u0105\":\"unicode\\u0106\"}",
                "{..,\"a\\\"b\u0105\":\"unicode\u0106\"}",
                Deltas.mapBuilder().put("a\"b\u0105", "unicode\u0106").deleteIfEmpty());
    }

    @Test
    public void testLiteralArray() {
        // simple arrays are encoded using a list literal
        doTest(
                "{..,\"tags\":[\"NEWBIE\"]}",
                "{..,\"tags\":[\"NEWBIE\"]}",
                Deltas.mapBuilder().put("tags", Collections.singletonList("NEWBIE")));
    }

    @Test
    public void testLiteralMap() {
        // simple maps are encoded using literal, not MapDelta
        doTest(
                "{\"tags\":[\"NEWBIE\"],\"photos\":3}",
                "{\"photos\":3,\"tags\":[\"NEWBIE\"]}",
                Deltas.literal(ImmutableMap.<String, Object>of("tags", Collections.singletonList("NEWBIE"), "photos", 3)));
    }

    @Test
    public void testInvalidArray() {
        doTestException("[..,\"NEWBIE\"]", "Expected a valid value");
    }

    @Test
    public void testNonStringKeys() {
        // non-string keys are not coerced to strings
        doTestException("{true:false,null:0,1:2}", "Expected '\"' and instead saw 't'");
        doTestException("{..,true:false,null:0,1:2}", "Expected '\"' and instead saw 't'");
    }

    @Test
    public void testDeleteIfEmptyOptimization() {
        doTest("{..,\"key\":~}?", "{..,\"key\":~}?", Deltas.mapBuilder().remove("key").deleteIfEmpty());
        doTest("{..,\"key\":{}?}?", "{..,\"key\":~}?", Deltas.mapBuilder().remove("key").deleteIfEmpty());
        doTest("{..,\"key\":{}}?", "{..,\"key\":{}}", Deltas.mapBuilder().put("key", ImmutableMap.of()).deleteIfEmpty());
        doTest("{..,\"key\":[]}?", "{..,\"key\":[]}", Deltas.mapBuilder().put("key", ImmutableList.of()).deleteIfEmpty());
        doTest("{..,\"1st\":{}?,\"2nd\":~}?", "{..,\"1st\":~,\"2nd\":~}?", Deltas.mapBuilder().removeAll("1st", "2nd").deleteIfEmpty());

        doTest(
                "{..,\"1st\":{}?,\"2nd\":[],\"3rd\":{\"1\":2}?,\"4th\":[3],\"5th\":{}}?",
                "{..,\"1st\":~,\"2nd\":[],\"3rd\":{\"1\":2},\"4th\":[3],\"5th\":{}}",
                Deltas.mapBuilder().
                        remove("1st").
                        put("2nd", ImmutableList.of()).
                        put("3rd", ImmutableMap.of("1", 2)).
                        put("4th", ImmutableList.of(3)).
                        put("5th", ImmutableMap.of()));
    }

    @Test
    public void testExtraCommas() {
        // Strict on JSON parsing to allow us more future flexibility in extending the delta language
        doTestException("[,1,,3,]", "Missing value");
        doTestException("{\"x\":1,,\"y\":2,]?", "Expected '\"' and instead saw ','");
        doTestException("{..,\"1\":2,,,\"3\":4,}", "Expected '\"' and instead saw ','");
    }

    @Test
    public void testDeltasInLists() {
        // Can't nest delta syntax within array values.  Maps within arrays get sketchy when conflicts must be
        // resolved because array indices aren't necessarily stable.
        doTest(
                "[{\"z\":[{\"a\":\"b\"}]}]",
                "[{\"z\":[{\"a\":\"b\"}]}]",
                Deltas.literal(Collections.singletonList(Collections.singletonMap("z", Collections.singletonList(Collections.singletonMap("a", "b"))))));
        doTestException("[{\"z\":[{..,\"a\":\"b\"}]}]", "Expected '\"' and instead saw '.'");
        // can nest delta syntax within map values
        doTestException("{\"z\":{.,\"a\":\"b\"}}", "Expected '.' and instead saw ','");
        doTestException("{\"z\":{...,\"a\":\"b\"}}", "Expected ',' or '}' and instead saw '.'");
        doTest("{\"z\":{..,\"a\":\"b\"}}", "{\"z\":{..,\"a\":\"b\"}}", Deltas.mapBuilder().update("z", Deltas.mapBuilder().put("a", "b").build()).removeRest());
    }

    @Test
    public void testMapDeletes() {
        // deleting keys and values
        doTest("{..,\"rating\":~}", "{..,\"rating\":~}", Deltas.mapBuilder().remove("rating"));
        doTest("{..,\"rating\":if 5 then ~ end}", "{..,\"rating\":if 5 then ~ end}", Deltas.mapBuilder().remove("rating", 5));
        doTest("{..,\"rating\":if {} then ~ end}", "{..,\"rating\":if {} then ~ end}", Deltas.mapBuilder().remove("rating", Collections.emptyMap()));
        doTest("{..,\"rating\":{}?}", "{..,\"rating\":~}", Deltas.mapBuilder().remove("rating"));
        doTestException("{..,\"rating\":if {}? then ~ end}", "Missing value");
        doTest("{\"rating\":~}", "{}", Deltas.literal(Collections.emptyMap()));
    }

    @Test
    public void testMapUpdateIfExists() {
        doTest("{..,\"photo\":if + then {..,\"status\":\"APPROVED\"} end}",
                "{..,\"photo\":if + then {..,\"status\":\"APPROVED\"} end}",
                Deltas.mapBuilder().updateIfExists("photo", Deltas.mapBuilder().put("status", "APPROVED").build()));
    }

    @Test
    public void testNoop() {
        doTest("..", "..", Deltas.noop());
    }

    @Test
    public void testUnquotedKeys() {
        doTestException("{1:2}", "Expected '\"' and instead saw '1'");
        doTestException("{true:2}", "Expected '\"' and instead saw 't'");
        doTestException("{null:2}", "Expected '\"' and instead saw 'n'");
        doTestException("{..,1:2}", "Expected '\"' and instead saw '1'");
        doTestException("{..,true:2}", "Expected '\"' and instead saw 't'");
        doTestException("{..,null:2}", "Expected '\"' and instead saw 'n'");
    }

    @Test
    public void testConditionalDeltas() {
        doTest("if is(undefined) then {\"rating\":5} end",
                "if ~ then {\"rating\":5} end",
                Deltas.conditional(
                        Conditions.is(State.UNDEFINED),
                        Deltas.mapBuilder().put("rating", 5).removeRest().build()));

        doTest("{..,\"published\":if {..,\"status\":\"APPROVED\"} then true else false end}",
                "{..,\"published\":if {..,\"status\":\"APPROVED\"} then true else false end}",
                Deltas.mapBuilder()
                        .update("published", Deltas.conditional(
                                Conditions.mapBuilder().contains("status", "APPROVED").build(),
                                Deltas.literal(true),
                                Deltas.literal(false))));

        doTest("{..,\"published\":if {..,\"status\":\"APPROVED\"} then true elif {..,\"status\":\"SUBMITTED\"} then false else ~ end}",
                "{..,\"published\":if {..,\"status\":\"APPROVED\"} then true elif {..,\"status\":\"SUBMITTED\"} then false else ~ end}",
                Deltas.mapBuilder()
                        .update("published", Deltas.conditionalBuilder()
                                .add(Conditions.mapBuilder().contains("status", "APPROVED").build(), Deltas.literal(true))
                                .add(Conditions.mapBuilder().contains("status", "SUBMITTED").build(), Deltas.literal(false))
                                .otherwise(Deltas.delete())
                                .build()));
    }

    @Test
    public void testComparisonDeltas() {
        doTest("if {..,\"objVersion\":le(5)} then {\"content\":\"replaced\",\"objVersion\":6} end",
                "if {..,\"objVersion\":le(5)} then {\"content\":\"replaced\",\"objVersion\":6} end",
                Deltas.conditional(
                        Conditions.mapBuilder()
                                .matches("objVersion", Conditions.le(5))
                                .build(),
                        Deltas.mapBuilder()
                                .put("content", "replaced")
                                .put("objVersion", 6)
                                .removeRest()
                                .build()));

        doTest("{..,\"certain\":if {..,\"confidence\":gt(99.9)} then true else false end}",
                "{..,\"certain\":if {..,\"confidence\":gt(99.9)} then true else false end}",
                Deltas.mapBuilder()
                        .update("certain", Deltas.conditional(
                                Conditions.mapBuilder().matches("confidence", Conditions.gt(99.9)).build(),
                                Deltas.literal(true),
                                Deltas.literal(false)))
                        .build());
    }

    @Test
    public void testContainmentDeltas() {
        doTest("if {..,\"badges\":contains(\"top5\")} then {..,\"badges\":(..,\"top10\")} end",
                "if {..,\"badges\":contains(\"top5\")} then {..,\"badges\":(..,\"top10\")} end",
                Deltas.conditional(
                        Conditions.mapBuilder()
                                .matches("badges", Conditions.contains("top5"))
                                .build(),
                        Deltas.mapBuilder()
                                .update("badges", Deltas.setBuilder()
                                        .add("top10")
                                        .build())
                                .build()));

        doTest("if {..,\"badges\":containsAny(\"top5\",\"top10\")} then {..,\"badges\":(..,\"top25\")} end",
                "if {..,\"badges\":containsAny(\"top10\",\"top5\")} then {..,\"badges\":(..,\"top25\")} end",
                Deltas.conditional(
                        Conditions.mapBuilder()
                                .matches("badges", Conditions.containsAny("top5", "top10"))
                                .build(),
                        Deltas.mapBuilder()
                                .update("badges", Deltas.setBuilder()
                                        .add("top25")
                                        .build())
                                .build()));

        doTest("if {..,\"awards\":containsAll(\"Emmy\",\"Grammy\",\"Oscar\",\"Tony\")} then {..,\"EGOT\":true} end",
                "if {..,\"awards\":containsAll(\"Emmy\",\"Grammy\",\"Oscar\",\"Tony\")} then {..,\"EGOT\":true} end",
                Deltas.conditional(
                        Conditions.mapBuilder()
                                .matches("awards", Conditions.containsAll("Emmy", "Grammy", "Oscar", "Tony"))
                                .build(),
                        Deltas.mapBuilder()
                                .put("EGOT", true)
                                .build()));

        doTest("if {..,\"bigFeatures\":containsOnly(\"eyes\",\"nose\",\"teeth\")} then {..,\"creature\":\"wolf\"} end",
                "if {..,\"bigFeatures\":containsOnly(\"eyes\",\"nose\",\"teeth\")} then {..,\"creature\":\"wolf\"} end",
                Deltas.conditional(
                        Conditions.mapBuilder()
                                .matches("bigFeatures", Conditions.containsOnly("eyes", "nose", "teeth"))
                                .build(),
                        Deltas.mapBuilder()
                                .put("creature", "wolf")
                                .build()));
    }

    @Test
    public void testSimpleSet() {
        doTest("(5, 3, 1, 2, 4)",
                "(1,2,3,4,5)",
                Deltas.setBuilder()
                        .addAll(1, 2, 3, 4, 5)
                        .removeRest());
        doTest("(..,5, 3, 1, 2, 4)",
                "(..,1,2,3,4,5)",
                Deltas.setBuilder()
                        .addAll(1, 2, 3, 4, 5));
    }

    @Test
    public void testSetRemove() {
        doTest("(..,~1, ~2, 3)",
                "(..,3,~1,~2)",
                Deltas.setBuilder()
                        .removeAll(1, 2)
                        .add(3));
        doTest("(..,~\"do\", ~\"re\", \"mi\")",
                "(..,\"mi\",~\"do\",~\"re\")",
                Deltas.setBuilder()
                        .removeAll("do", "re")
                        .add("mi"));
    }

    @Test
    public void testHeterogeneousSet() {
        doTest("(..,2.5, 1, \"do\", 123123123123123123, \"re\", false, null, \"mi\", 5, {\"a\":1}, true, [5, 7, 9])",
                "(..,null,false,true,[5,7,9],{\"a\":1},1,2.5,5,123123123123123123,\"do\",\"mi\",\"re\")",
                Deltas.setBuilder()
                        .addAll(1, 2.5, 5, 123123123123123123L)
                        .addAll(false, true)
                        .add(null)
                        .addAll("do", "re", "mi")
                        .add(ImmutableMap.<String, Object>of("a", 1))
                        .add(ImmutableList.of(5, 7, 9)));
    }

    @Test
    public void testInvalidSetDeltas() {
        doTestException("(..,{..,\"foo\":\"bar\"})", "Non-literal values not supported in sets");
        doTestException("(..,1,~1)", "Multiple operations against the same value are not allowed");
    }

    private void doTest(String input, String expectedString, DeltaBuilder expected) {
        doTest(input, expectedString, expected.build());
    }

    private void doTest(String input, String expectedString, Delta expected) {
        Delta actual = Deltas.fromString(input);
        assertEquals(actual, expected, "DeltaParser returned unexpected results:\nGiven   : " + input);
        assertEquals(actual.toString(), expectedString);  // note: Delta.toString() must produce sorted, deterministic output
    }

    private void doTestException(String input, String message) {
        try {
            Deltas.fromString(input);
            fail("Expected exception on input: " + input);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(message), "Incorrect exception on input: " + input + "\nActual  : " + e.toString() + "\nExpected: " + message);
        }
    }
}
