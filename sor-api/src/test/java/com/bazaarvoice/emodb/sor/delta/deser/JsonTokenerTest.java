package com.bazaarvoice.emodb.sor.delta.deser;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;

/**
 * Adapted from {@code org.json.Test}.  Removed all JsonML, XML etc. tests and converted the ones that test parsing
 * into unit tests.
 */
public class JsonTokenerTest {

    @Test
    public void testBackAfterEof() {
        JsonTokener t = new JsonTokener("x");
        assertEquals(t.next(), 'x');
        assertEquals(t.next(), 0);
        t.back();
        assertEquals(t.next(), 0);
    }

    @Test
    public void testDoubleList() {
        assertEquals(parse("[0.1]"), ImmutableList.of(0.1D));
    }

    @Test
    public void testLongList() {
        assertEquals(parse("[12345678901]"), ImmutableList.of(12345678901L));
    }

    @Test
    public void testWhitespace() {
        assertEquals(parse("{     \"list of lists\" : [         [1, 2, 3],         [4, 5, 6]     ] }"),
                ImmutableMap.of("list of lists", ImmutableList.of(ImmutableList.of(1, 2, 3), ImmutableList.of(4, 5, 6))));
    }

    @Test
    public void testSimpleEntity() {
        assertEquals(
                parse("{ \"entity\": { \"imageURL\": \"\", \"name\": \"IXXXXXXXXXXXXX\", \"id\": 12336, \"ratingCount\": null, \"averageRating\": null } }"),
                ImmutableMap.of("entity", new NullableMapBuilder()
                        .put("imageURL", "")
                        .put("name", "IXXXXXXXXXXXXX")
                        .put("id", 12336)
                        .put("ratingCount", null)
                        .put("averageRating", null)
                        .build()));
    }

    @Test
    public void testTypes() {
        List<Object> list = new NullableListBuilder()
                .add(1)
                .add(new NullableListBuilder()
                        .add(null)
                        .add(new NullableListBuilder()
                                .add(new NullableMapBuilder()
                                        .put("empty-array", ImmutableList.of())
                                        .put("answer", 42)
                                        .put("null", null)
                                        .put("false", false)
                                        .put("true", true)
                                        .put("big", 123456789e+88)
                                        .put("small", 123456789e-88)
                                        .put("empty-object", ImmutableMap.of())
                                        .put("long", 9223372036854775807L)
                                        .build())
                                .add("two")
                                .build())
                        .add(true)
                        .build())
                .add(98.6)
                .add(-100.0)
                .add(ImmutableMap.of())
                .add(ImmutableMap.of("one", 1.00))
                .build();
        assertEquals(parse(JsonHelper.asJson(list)), list);
    }

    @Test
    public void testEscapes() {
        assertEquals(
                parse("{\"slashes\": \"///\", " +
                        "\"closetag\": \"</script>\", " +
                        "\"backslash\":\"\\\\\", " +
                        "\"ei\": {\"quotes\": \"\\\"\\'\"}," +
                        "\"eo\": {\"a\": \"\\\"quoted\\\"\", \"b\":\"don't\"}, " +
                        "\"quotes\": [\"'\", \"\\\"\"]}"),
                ImmutableMap.builder()
                        .put("slashes", "///")
                        .put("closetag", "</script>")
                        .put("backslash", "\\")
                        .put("ei", ImmutableMap.of("quotes", "\"'"))
                        .put("eo", ImmutableMap.of("a", "\"quoted\"", "b", "don't"))
                        .put("quotes", ImmutableList.of("'", "\""))
                        .build());
    }

    @Test
    public void testTypes2() {
        assertEquals(
                new JsonTokener("{\"foo\": [true, false,9876543210,    0.0, 1.00000001,  1.000000000001, 1.00000000000000001," +
                        " 0.00000000000000001, 2.00, 0.1, 2e100, -32,[],{}, \"string\"], " +
                        "  \"to\"   : null, \"op\" : \"Good\"," +
                        "\"ten\":10} postfix comment").nextValue(),
                new NullableMapBuilder()
                        .put("foo", ImmutableList.of(true, false, 9876543210L, 0.0, 1.00000001, 1.000000000001, 1.00000000000000001,
                                0.00000000000000001, 2.00, 0.1, 2e100, -32, ImmutableList.of(), ImmutableMap.of(), "string"))
                        .put("to", null)
                        .put("op", "Good")
                        .put("ten", 10)
                        .build());
    }

    @Test
    public void testTypes3() {
        assertEquals(
                parse("{\"foo\": [true, false,9876543210,    0.0, 1.00000001,  1.000000000001, 1.00000000000000001," +
                        " 0.00000000000000001, 2.00, 0.1, 2e100, -32,[],{}, \"string\"," +
                        "666, 2001.99, \"so \\\"fine\\\".\", \"so <fine>.\", true, false,[], {}], " +
                        "  \"to\"   : null, \"op\" : \"Good\"," +
                        "\"ten\":10," +
                        "\"JSONArray\": []," +
                        "\"JSONObject\": {}," +
                        "\"int\": 57," +
                        "\"keys\": [\"to\", \"ten\", \"JSONObject\", \"JSONArray\", \"op\", \"int\", \"true\", \"foo\", \"zero\", \"double\", \"String\", \"false\", \"bool\", \"\\\\u2028\", \"\\\\u2029\", \"null\"]," +
                        "\"null\": null," +
                        "\"bool\": \"true\"," +
                        "\"true\": true," +
                        "\"false\": false," +
                        "\"zero\": -0," +
                        "\"double\": 123456789012345678901234567890.," +
                        "\"String\": \"98.6\"," +
                        "\"\\\\u2028\": \"\\u2028\"," +
                        "\"\\\\u2029\": \"\\u2029\"}"),
                new NullableMapBuilder()
                        .put("foo", ImmutableList.of(true, false, 9876543210L, 0.0, 1.00000001, 1.000000000001, 1.00000000000000001,
                                0.00000000000000001, 2.00, 0.1, 2e100, -32, ImmutableList.of(), ImmutableMap.of(), "string",
                                666, 2001.99, "so \"fine\".", "so <fine>.", true, false, ImmutableList.of(), ImmutableMap.of()))
                        .put("to", null)
                        .put("op", "Good")
                        .put("ten", 10)
                        .put("JSONArray", ImmutableList.of())
                        .put("JSONObject", ImmutableMap.of())
                        .put("int", 57)
                        .put("keys", ImmutableList.of("to", "ten", "JSONObject", "JSONArray", "op", "int", "true", "foo", "zero", "double", "String", "false", "bool", "\\u2028", "\\u2029", "null"))
                        .put("null", null)
                        .put("bool", "true")
                        .put("true", true)
                        .put("false", false)
                        .put("zero", 0)
                        .put("double", 1.2345678901234568E29)
                        .put("String", "98.6")
                        .put("\\u2028", "\u2028")
                        .put("\\u2029", "\u2029")
                        .build());
    }

    @Test
    public void testMapNumerics() {
        assertEquals(
                parse("{\"string\": \"98.6\", \"long\": 2147483648, \"int\": 2147483647, \"longer\": 9223372036854775807, \"double\": 9223372036854775808}"),
                ImmutableMap.of("string", "98.6", "long", 2147483648L, "int", 2147483647, "longer", 9223372036854775807L, "double", 9223372036854775808D));
    }

    @Test
    public void testListNumerics() {
        assertEquals(
                parse("[2147483647, 2147483648, 9223372036854775807, 9223372036854775808]"),
                ImmutableList.of(2147483647, 2147483648L, 9223372036854775807L, 9223372036854775808D));
    }

    @Test
    public void testNegativeInfinity() {
        assertEquals(parse(JsonHelper.asJson(ImmutableList.of(Double.NEGATIVE_INFINITY))), ImmutableList.of("-Infinity"));
    }

    @Test
    public void testNaN() {
        assertEquals(parse(JsonHelper.asJson(ImmutableList.of(Double.NaN))), ImmutableList.of("NaN"));
    }

    @Test
    public void testControlEscapes() {
        assertEquals(parse("\"ab\\u0063d\\u0000\\u001f\""), "abcd\u0000\u001f");
    }

    @Test
    public void testBadNegativeInt() {
        assertEquals(parse("-1"), -1);
    }

    @Test
    public void testBadNegativeDouble() {
        assertEquals(parse("-1.0"), -1.0);
    }

    @Test(expectedExceptions = ParseException.class)
    public void testBadTabInString() {
        parse("\"\t\"");
    }

    @Test(expectedExceptions = ParseException.class)
    public void testBadLeadingDecimal() {
        parse(".001");
    }

    @Test(expectedExceptions = ParseException.class)
    public void testBadLeadingSign() {
        parse("+1");
    }

    @Test(expectedExceptions = ParseException.class)
    public void testBadSingleQuotedString() {
        parse("'foo'");
    }

    @Test(expectedExceptions = ParseException.class)
    public void testBadListTrailingComma() {
        parse("[1,2,]");
    }

    @Test(expectedExceptions = ParseException.class)
    public void testBadMapTrailingComma() {
        parse("{\"x\":1,}");
    }

    @Test(expectedExceptions = ParseException.class)
    public void testBadImpliedNull() {
        parse(" [\"<escape>\", \"next is an implied null\" , , \"ok\",] ");
    }

    @Test(expectedExceptions = ParseException.class)
    public void testBadArrayEnd() {
        parse("[\n\r\n\r}");
    }

    @Test(expectedExceptions = ParseException.class)
    public void testBadStart() {
        new JsonTokener("<\n\r\n\r      ").nextArray();
    }

    @Test(expectedExceptions = ParseException.class)
    public void testBadArrayEnd2() {
        parse("[)");
    }

    @Test(expectedExceptions = ParseException.class)
    public void testBadObjectEnd() {
        parse("{]");
    }

    @Test(expectedExceptions = ParseException.class)
    public void testDuplicateEntry() {
        parse("{\"koda\": true, \"koda\": true}");
    }

    @Test(expectedExceptions = ParseException.class)
    public void testDuplicateKey() {
        parse("{\"bosanda\": \"MARIE HAA'S\", \"bosanda\": \"MARIE HAA\\\\'S\"}");
    }

    private Object parse(String string) {
        JsonTokener t = new JsonTokener(string);
        Object value = t.nextValue();
        if (t.nextClean() != 0) {
            throw t.syntaxError("Unexpected characters at the end of the string");
        }
        return value;
    }

    private static class NullableListBuilder {
        private final List<Object> _list = Lists.newArrayList();

        public NullableListBuilder add(@Nullable Object value) {
            _list.add(value);
            return this;
        }

        public List<Object> build() {
            return _list;
        }
    }

    private static class NullableMapBuilder {
        private final Map<String, Object> _map = Maps.newLinkedHashMap();

        public NullableMapBuilder put(String key, @Nullable Object value) {
            _map.put(key, value);
            return this;
        }

        public Map<String, Object> build() {
            return _map;
        }
    }
}
