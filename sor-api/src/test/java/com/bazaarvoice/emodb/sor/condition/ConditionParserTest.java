package com.bazaarvoice.emodb.sor.condition;

import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class ConditionParserTest {
   
    @Test
    public void testAlwaysTrue() {
        doTest("alwaysTrue()", "alwaysTrue()", Conditions.alwaysTrue());
        assertEquals(Conditions.always(true), Conditions.alwaysTrue());
    }

    @Test
    public void testAlwaysFalse() {
        doTest("alwaysFalse()", "alwaysFalse()", Conditions.alwaysFalse());
        assertEquals(Conditions.always(false), Conditions.alwaysFalse());
    }

    @Test
    public void testEqual() {
        doTest("null", "null", Conditions.equal(null));
        doTest("true", "true", Conditions.equal(true));
        doTest("false", "false", Conditions.equal(false));
        doTest("-1234", "-1234", Conditions.equal(-1234));
        doTest("-1234.56", "-1234.56", Conditions.equal(-1234.56));
        doTest("\"abc\"", "\"abc\"", Conditions.equal("abc"));
        doTest("[]", "[]", Conditions.equal(ImmutableList.of()));
        doTest("[\"abc\",1,true]", "[\"abc\",1,true]", Conditions.equal(ImmutableList.of("abc", 1, true)));
        doTest("{}", "{}", Conditions.equal(ImmutableMap.of()));
        doTest("{\"x\":1,\"a\":2}", "{\"a\":2,\"x\":1}", Conditions.equal(ImmutableMap.of("x", 1, "a", 2)));
    }

    @Test
    public void testIn() {
        doTest("in()", "alwaysFalse()", Conditions.in());
        doTest("in(null)", "null", Conditions.in((Object) null));
        doTest("in(1)", "1", Conditions.in(1));
        doTest("in(1,1,1)", "1", Conditions.equal(1));
        doTest("in(true,1,null)", "in(1,null,true)", Conditions.in(true, 1, null));
        doTest("in({\"x\":1,\"a\":2},[5,6],\"xyz\")", "in(\"xyz\",[5,6],{\"a\":2,\"x\":1})",
                Conditions.in(ImmutableMap.of("x", 1, "a", 2), ImmutableList.of(5, 6), "xyz"));
    }

    @Test
    public void testIntrinsics() {
        doTest("intrinsic( \"~id\" : \"abc\" )", "intrinsic(\"~id\":\"abc\")", Conditions.intrinsic(Intrinsic.ID, "abc"));
        doTestException("intrinsic(\"~version\":2)", "~version");
        doTest("intrinsic(\"~deleted\":true)", "intrinsic(\"~deleted\":true)", Conditions.intrinsic(Intrinsic.DELETED, true));
        doTest("intrinsic(\"~deleted\":false)", "intrinsic(\"~deleted\":false)", Conditions.intrinsic(Intrinsic.DELETED, false));
        doTest("intrinsic(\"~signature\":\"1234567890abcdef\")", "intrinsic(\"~signature\":\"1234567890abcdef\")", Conditions.intrinsic(Intrinsic.SIGNATURE, "1234567890abcdef"));
        doTest("intrinsic(\"~firstUpdateAt\":null)", "intrinsic(\"~firstUpdateAt\":null)", Conditions.intrinsic(Intrinsic.FIRST_UPDATE_AT, (Object) null));
        doTest("intrinsic(\"~firstUpdateAt\":\"2012-07-08T22:21:36.866Z\")", "intrinsic(\"~firstUpdateAt\":\"2012-07-08T22:21:36.866Z\")", Conditions.intrinsic(Intrinsic.FIRST_UPDATE_AT, "2012-07-08T22:21:36.866Z"));
        doTest("intrinsic(\"~lastUpdateAt\":null)", "intrinsic(\"~lastUpdateAt\":null)", Conditions.intrinsic(Intrinsic.LAST_UPDATE_AT, (Object) null));
        doTest("intrinsic(\"~table\":\"abc\",\"xyz\",\"foo\")", "intrinsic(\"~table\":\"abc\",\"foo\",\"xyz\")",
                Conditions.intrinsic(Intrinsic.TABLE, Conditions.or(Conditions.equal("abc"), Conditions.equal("xyz"), Conditions.equal("foo"))));
    }

    @Test
    public void testIs() {
        doTest("~", "~", Conditions.isUndefined());
        doTest("+", "+", Conditions.isDefined());
        doTestException("is()", "Missing value");
        doTest("is ( undefined ) ", "~", Conditions.isUndefined());
        doTest("is(defined)", "+", Conditions.isDefined());
        doTest("is(null)", "null", Conditions.isNull());
        doTest("is(bool)", "is(bool)", Conditions.isBoolean());
        doTest("is(num)", "is(num)", Conditions.isNumber());
        doTest("is(string)", "is(string)", Conditions.isString());
        doTest("is(array)", "is(array)", Conditions.isList());
        doTest("is(object)", "is(object)", Conditions.isMap());
        doTestException("is(num,null)", "Expected ')' and instead saw ','");

        assertEquals(Conditions.isNull(), Conditions.equal(null));
        assertEquals(Conditions.isNull().toString(), "null");
    }

    @Test
    public void testNot() {
        doTest("not(1)", "not(1)", Conditions.not(Conditions.equal(1)));
        doTest("not(not(false))", "false", Conditions.equal(false));
    }

    @Test
    public void testOr() {
        doTest("or()", "alwaysFalse()", Conditions.alwaysFalse());
        doTest("or(alwaysFalse())", "alwaysFalse()", Conditions.alwaysFalse());
        doTest("or(alwaysTrue())", "alwaysTrue()", Conditions.alwaysTrue());
        doTest("or(1)", "1", Conditions.equal(1));
        doTest("or(1, alwaysFalse())", "1", Conditions.equal(1));
        doTest("or(1, alwaysTrue())", "alwaysTrue()", Conditions.alwaysTrue());
        doTest("or(2,1)", "in(1,2)", Conditions.or(Conditions.equal(1), Conditions.equal(2)));
        doTest("or([],{})", "in([],{})", Conditions.or(Conditions.equal(ImmutableList.of()), Conditions.equal(ImmutableMap.of())));
        doTest("or([],{..})", "or([],{..})", Conditions.or(Conditions.equal(ImmutableList.of()), Conditions.mapBuilder().build()));
        doTest("or(1,{..},2)", "or(in(1,2),{..})", Conditions.or(Conditions.equal(1), Conditions.equal(2), Conditions.mapBuilder().build()));
        doTest("or(intrinsic(\"~id\":\"xyz\"),intrinsic(\"~id\":\"abc\"))", "intrinsic(\"~id\":\"abc\",\"xyz\")",
                Conditions.intrinsic(Intrinsic.ID, Conditions.in("abc", "xyz")));
        doTest("or(1,{..},2,intrinsic(\"~id\":\"xyz\"),intrinsic(\"~id\":\"abc\"),intrinsic(\"~table\":\"tbl\"))",
                "or(in(1,2),intrinsic(\"~id\":\"abc\",\"xyz\"),intrinsic(\"~table\":\"tbl\"),{..})",
                Conditions.or(Conditions.in(1, 2), Conditions.intrinsic(Intrinsic.ID, Conditions.in("abc", "xyz")),
                        Conditions.intrinsic(Intrinsic.TABLE, "tbl"), Conditions.mapBuilder().build()));
    }

    @Test
    public void testAnd() {
        doTest("and()", "alwaysTrue()", Conditions.alwaysTrue());
        doTest("and(1)", "1", Conditions.equal(1));
        doTest("and(1,2)", "and(1,2)", Conditions.and(Conditions.equal(1), Conditions.equal(2)));
        doTest("and([],{})", "and([],{})", Conditions.and(Conditions.equal(ImmutableList.of()), Conditions.equal(ImmutableMap.of())));
        doTest("and([],{..})", "and([],{..})", Conditions.and(Conditions.equal(ImmutableList.of()), Conditions.mapBuilder().build()));
    }

    @Test
    public void testComparison() {
        doTest("gt(20)", "gt(20)", Conditions.gt(20));
        doTest("ge(55.12345)", "ge(55.12345)", Conditions.ge(55.12345));
        doTest("lt(999)", "lt(999)", Conditions.lt(999));
        doTest("le(0.875236)", "le(0.875236)", Conditions.le(0.875236));
        doTest("ge(\"hello\")", "ge(\"hello\")", Conditions.ge("hello"));
        doTestException("gt({\"map\":\"unsupported\"})", "gt only supports numbers and strings");
    }

    @Test
    public void testContains() {
        doTest("contains(5)", "contains(5)", Conditions.contains(5));
        doTest("contains(null)", "contains(null)", Conditions.contains(null));
        doTest("containsAny()", "alwaysTrue()", Conditions.alwaysTrue());
        doTest("containsAny(\"solo\")", "contains(\"solo\")", Conditions.contains("solo"));
        doTest("containsAny(\"high\", \"low\")", "containsAny(\"high\",\"low\")", Conditions.containsAny("high", "low"));
        doTest("containsAll()", "alwaysTrue()", Conditions.alwaysTrue());
        doTest("containsAll(\"solo\")", "contains(\"solo\")", Conditions.contains("solo"));
        doTest("containsAll(\"tall\", \"dark\", \"handsome\")", "containsAll(\"dark\",\"handsome\",\"tall\")",
                Conditions.containsAll("tall", "dark", "handsome"));
        doTest("containsOnly()", "containsOnly()", Conditions.containsOnly());
        doTest("containsOnly(\"solo\")", "containsOnly(\"solo\")", Conditions.containsOnly("solo"));
        doTest("containsOnly(1, 2, 3)", "containsOnly(1,2,3)", Conditions.containsOnly(1, 2, 3));
    }

    @Test
    public void testLike() {
        doTest("like(\"prefix*\")", "like(\"prefix*\")", Conditions.like("prefix*"));
        doTest("like(\"*suffix\")", "like(\"*suffix\")", Conditions.like("*suffix"));
        doTest("like(\"*contains*\")", "like(\"*contains*\")", Conditions.like("*contains*"));
        doTest("like(\"*1*2*3*\")", "like(\"*1*2*3*\")", Conditions.like("*1*2*3*"));
        doTest("like(\"wild*notwild\\\\*\")", "like(\"wild*notwild\\\\*\")", Conditions.like("wild*notwild\\*"));
        doTest("like(\"*\")", "is(string)", Conditions.isString());
        doTest("like(\"***\")", "is(string)", Conditions.isString());
        doTest("like(\"const\")", "\"const\"", Conditions.equal("const"));
    }

    private void doTest(String input, String expectedString, Condition expected) {
        Condition actual = Conditions.fromString(input);
        assertEquals(actual, expected, "ConditionParser returned unexpected results:\nGiven   : " + input);
        assertEquals(actual.toString(), expectedString);  // note: Condition.toString() must produce sorted, deterministic output
    }

    private void doTestException(String input, String message) {
        try {
            Conditions.fromString(input);
            fail("Expected exception on input: " + input);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(message), "Incorrect exception on input: " + input + "\nActual  : " + e.toString() + "\nExpected: " + message);
        }
    }
}
