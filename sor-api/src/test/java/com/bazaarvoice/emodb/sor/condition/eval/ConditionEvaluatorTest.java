package com.bazaarvoice.emodb.sor.condition.eval;

import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.condition.AndCondition;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.condition.MapCondition;
import com.bazaarvoice.emodb.sor.condition.OrCondition;
import com.bazaarvoice.emodb.sor.condition.State;
import com.bazaarvoice.emodb.sor.condition.impl.LikeConditionImpl;
import com.bazaarvoice.emodb.sor.delta.eval.DeltaEvaluator;
import com.bazaarvoice.emodb.sor.delta.eval.Intrinsics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ConditionEvaluatorTest {

    @Test
    public void testAlwaysTrue() {
        assertTrue(eval(Conditions.alwaysTrue(), DeltaEvaluator.UNDEFINED));
        assertTrue(eval(Conditions.alwaysTrue(), null));
        assertTrue(eval(Conditions.alwaysTrue(), true));
        assertTrue(eval(Conditions.alwaysTrue(), false));
        assertTrue(eval(Conditions.alwaysTrue(), ImmutableMap.of("foo", 1)));
    }

    @Test
    public void testAlwaysFalse() {
        assertFalse(eval(Conditions.alwaysFalse(), DeltaEvaluator.UNDEFINED));
        assertFalse(eval(Conditions.alwaysFalse(), null));
        assertFalse(eval(Conditions.alwaysFalse(), true));
        assertFalse(eval(Conditions.alwaysFalse(), false));
        assertFalse(eval(Conditions.alwaysFalse(), ImmutableMap.of("foo", 1)));
    }

    @Test
    public void testNot() {
        assertFalse(eval(Conditions.not(Conditions.alwaysTrue()), null));
        assertTrue(eval(Conditions.not(Conditions.alwaysFalse()), null));
    }

    @Test
    public void testOr() {
        assertFalse(eval(Conditions.or(), true));
        assertFalse(eval(Conditions.or(Conditions.equal(1)), 2));
        assertFalse(eval(Conditions.or(Conditions.equal(1), Conditions.equal(2)), 3));
        assertTrue(eval(Conditions.or(Conditions.equal(1), Conditions.equal(2)), 2));

        assertEquals(
                Conditions.orBuilder().or(Conditions.equal(1)).or(Conditions.equal(2)).build(),
                Conditions.or(Conditions.equal(1), Conditions.equal(2)));
    }

    @Test
    public void testIn() {
        assertFalse(eval(Conditions.in(), true));
        assertFalse(eval(Conditions.in(1), 2));
        assertFalse(eval(Conditions.in(1, 2), 3));
        assertTrue(eval(Conditions.in(1, 2), 2));
        assertFalse(eval(Conditions.in(1, 2), null));
        assertTrue(eval(Conditions.in(null, true, "string"), null));
        assertTrue(eval(Conditions.in(null, true, "string"), true));
        assertTrue(eval(Conditions.in(null, true, "string"), "string"));
        assertFalse(eval(Conditions.in(null, true, "string"), 5));
    }

    @Test
    public void testAnd() {
        assertTrue(eval(Conditions.and(), true));
        assertFalse(eval(Conditions.and(Conditions.equal(1)), 2));
        assertFalse(eval(Conditions.and(Conditions.equal(1), Conditions.equal(2)), 2));
        assertTrue(eval(Conditions.and(Conditions.is(State.NUM), Conditions.equal(2)), 2));

        assertEquals(
                Conditions.andBuilder().and(Conditions.is(State.NUM)).and(Conditions.equal(2)).build(),
                Conditions.and(Conditions.is(State.NUM), Conditions.equal(2)));
    }

    @Test
    public void testIntrinsicId() {
        Intrinsics intrinsics = mock(Intrinsics.class);
        when(intrinsics.getId()).thenReturn("abc");
        assertTrue(eval(Conditions.intrinsic(Intrinsic.ID, "abc"), null, intrinsics));
        assertFalse(eval(Conditions.intrinsic(Intrinsic.ID, "def"), null, intrinsics));
        assertFalse(eval(Conditions.intrinsic(Intrinsic.ID, Conditions.alwaysFalse()), null, intrinsics));
        assertTrue(eval(Conditions.intrinsic(Intrinsic.ID, Conditions.alwaysTrue()), null, intrinsics));
        assertFalse(eval(Conditions.intrinsic(Intrinsic.ID, Conditions.in("xyz", "123")), null, intrinsics));
        assertTrue(eval(Conditions.intrinsic(Intrinsic.ID, Conditions.in("xyz", "abc")), null, intrinsics));
        assertTrue(eval(Conditions.intrinsic(Intrinsic.ID, Conditions.not(Conditions.in("xyz", "123"))), null, intrinsics));
        assertFalse(eval(Conditions.intrinsic(Intrinsic.ID, Conditions.not(Conditions.in("xyz", "abc"))), null, intrinsics));
    }

    @Test
    public void testIntrinsicTable() {
        Intrinsics intrinsics = mock(Intrinsics.class);
        when(intrinsics.getTable()).thenReturn("review");
        assertTrue(eval(Conditions.intrinsic(Intrinsic.TABLE, "review"), null, intrinsics));
        assertFalse(eval(Conditions.intrinsic(Intrinsic.TABLE, "question"), null, intrinsics));
        assertTrue(eval(Conditions.intrinsic(Intrinsic.TABLE, Conditions.like("re*iew")), null, intrinsics));
    }

    @Test
    public void testIntrinsicDeletedFalse() {
        Intrinsics intrinsics = mock(Intrinsics.class);
        when(intrinsics.isDeleted()).thenReturn(false);
        assertTrue(eval(Conditions.intrinsic(Intrinsic.DELETED, false), null, intrinsics));
        assertFalse(eval(Conditions.intrinsic(Intrinsic.DELETED, true), null, intrinsics));
    }

    @Test
    public void testIntrinsicDeletedTrue() {
        Intrinsics intrinsics = mock(Intrinsics.class);
        when(intrinsics.isDeleted()).thenReturn(true);
        assertTrue(eval(Conditions.intrinsic(Intrinsic.DELETED, true), null, intrinsics));
        assertFalse(eval(Conditions.intrinsic(Intrinsic.DELETED, false), null, intrinsics));
    }

    @Test
    public void testIntrinsicFirstUpdateAtNull() {
        Intrinsics intrinsics = mock(Intrinsics.class);
        when(intrinsics.getFirstUpdateAt()).thenReturn(null);
        assertTrue(eval(Conditions.intrinsic(Intrinsic.FIRST_UPDATE_AT, (Object) null), null, intrinsics));
        assertFalse(eval(Conditions.intrinsic(Intrinsic.FIRST_UPDATE_AT, "2012-07-08T22:21:36.866Z"), null, intrinsics));
    }

    @Test
    public void testIntrinsicFirstUpdateAt() {
        Intrinsics intrinsics = mock(Intrinsics.class);
        when(intrinsics.getFirstUpdateAt()).thenReturn("2012-07-08T22:21:36.866Z");
        assertTrue(eval(Conditions.intrinsic(Intrinsic.FIRST_UPDATE_AT, "2012-07-08T22:21:36.866Z"), null, intrinsics));
        assertFalse(eval(Conditions.intrinsic(Intrinsic.FIRST_UPDATE_AT, "2012-07-08T22:29:31.588Z"), null, intrinsics));
        assertFalse(eval(Conditions.intrinsic(Intrinsic.FIRST_UPDATE_AT, (Object) null), null, intrinsics));
    }

    @Test
    public void testIntrinsicLastMutateAtNull() {
        Intrinsics intrinsics = mock(Intrinsics.class);
        when(intrinsics.getLastMutateAt()).thenReturn(null);
        assertTrue(eval(Conditions.intrinsic(Intrinsic.LAST_MUTATE_AT, (Object) null), null, intrinsics));
        assertFalse(eval(Conditions.intrinsic(Intrinsic.LAST_MUTATE_AT, "2012-07-08T22:21:36.866Z"), null, intrinsics));
    }

    @Test
    public void testIntrinsicLastUpdateAtNull() {
        Intrinsics intrinsics = mock(Intrinsics.class);
        when(intrinsics.getLastUpdateAt()).thenReturn(null);
        assertTrue(eval(Conditions.intrinsic(Intrinsic.LAST_UPDATE_AT, (Object) null), null, intrinsics));
        assertFalse(eval(Conditions.intrinsic(Intrinsic.LAST_UPDATE_AT, "2012-07-08T22:21:36.866Z"), null, intrinsics));
    }

    @Test
    public void testIntrinsicLastMutateAt() {
        Intrinsics intrinsics = mock(Intrinsics.class);
        when(intrinsics.getLastMutateAt()).thenReturn("2012-07-08T22:21:36.866Z");
        assertTrue(eval(Conditions.intrinsic(Intrinsic.LAST_MUTATE_AT, "2012-07-08T22:21:36.866Z"), null, intrinsics));
        assertFalse(eval(Conditions.intrinsic(Intrinsic.LAST_MUTATE_AT, "2012-07-08T22:29:31.588Z"), null, intrinsics));
        assertFalse(eval(Conditions.intrinsic(Intrinsic.LAST_MUTATE_AT, (Object) null), null, intrinsics));
    }

    @Test
    public void testIntrinsicLastUpdateAt() {
        Intrinsics intrinsics = mock(Intrinsics.class);
        when(intrinsics.getLastUpdateAt()).thenReturn("2012-07-08T22:21:36.866Z");
        assertTrue(eval(Conditions.intrinsic(Intrinsic.LAST_UPDATE_AT, "2012-07-08T22:21:36.866Z"), null, intrinsics));
        assertFalse(eval(Conditions.intrinsic(Intrinsic.LAST_UPDATE_AT, "2012-07-08T22:29:31.588Z"), null, intrinsics));
        assertFalse(eval(Conditions.intrinsic(Intrinsic.LAST_UPDATE_AT, (Object) null), null, intrinsics));
    }

    @Test
    public void testComparisons() {
        assertTrue(eval(Conditions.gt(Long.MAX_VALUE-1), Long.MAX_VALUE));
        assertTrue(eval(Conditions.gt(27), 28));
        assertTrue(eval(Conditions.ge(27), 28));
        assertFalse(eval(Conditions.le(27), 28));
        assertFalse(eval(Conditions.lt(27), 28));
        assertFalse(eval(Conditions.gt(55.5), 55.5));
        assertTrue(eval(Conditions.ge(55.5), 55.5));
        assertTrue(eval(Conditions.le(55.5), 55.5));
        assertFalse(eval(Conditions.lt(55.5), 55.5));
        assertTrue(eval(Conditions.gt("aaa"), "bbb"));
        assertTrue(eval(Conditions.ge("ccc"), "ccc"));
        assertTrue(eval(Conditions.le("ccc"), "ccc"));
        assertTrue(eval(Conditions.lt("zzz"), "yyy"));

        // If the previous value does not exist then the result is always false.
        assertFalse(eval(Conditions.gt(22), null));
        assertFalse(eval(Conditions.gt(49.7), null));

        // If the previous value cannot logically be compared then the result is always false.
        assertFalse(eval(Conditions.le("5"), 5));
        assertFalse(eval(Conditions.le(5), "5"));
        assertFalse(eval(Conditions.le(5), ImmutableMap.of("key", "value")));
        assertFalse(eval(Conditions.le(5), ImmutableList.of(1, 2, 3)));
    }

    @Test
    public void testEventTags() {
        Condition testCondition = Conditions.and(Conditions.mapBuilder().contains("type","review").build(),
                Conditions.not(Conditions.mapBuilder().matches("~tags",Conditions.containsAny("ignore")).build()));
        assertTrue(eval(testCondition,
                ImmutableMap.of("some","value","type","review","~tags",ImmutableList.of("ETL"))));
        assertFalse(eval(testCondition,
                ImmutableMap.of("some", "value", "type", "review", "~tags", Sets.newHashSet("ignore", "ETL"))));
        assertTrue(eval(testCondition,
                ImmutableMap.of("some","value","type","review")));


    }

    @Test
    public void testContains() {
        assertTrue(eval(Conditions.contains(3), ImmutableList.of(1, 2, 3)));
        assertTrue(eval(Conditions.containsAny("do", "re"), ImmutableList.of("do", "re")));
        assertTrue(eval(Conditions.containsAny("do", "re"), ImmutableList.of("re", "mi")));
        assertTrue(eval(Conditions.containsAll("do", "re"), ImmutableList.of("do", "re")));
        assertTrue(eval(Conditions.containsAll("do", "re"), ImmutableList.of("do", "re", "mi")));
        assertTrue(eval(Conditions.containsOnly("do", "re"), ImmutableList.of("do", "re")));
        assertTrue(eval(Conditions.containsOnly(), ImmutableList.of()));
        assertFalse(eval(Conditions.contains(3), ImmutableList.of()));
        assertFalse(eval(Conditions.contains(3), ImmutableList.of("one", "two", "three")));
        assertFalse(eval(Conditions.containsAny("do", "re"), ImmutableList.of("mi", "fa")));
        assertFalse(eval(Conditions.containsAll("do", "re"), ImmutableList.of("re", "mi", "fa")));
        assertFalse(eval(Conditions.containsOnly("do", "re"), ImmutableList.of("mi", "fa")));
        assertFalse(eval(Conditions.containsOnly("do", "re"), ImmutableList.of("do", "re", "mi")));
        assertTrue(eval(Conditions.contains(null), Arrays.asList(new Object[] { null })));
        assertTrue(eval(Conditions.containsAny((Object) null), Arrays.asList(null, "Bob")));
        assertTrue(eval(Conditions.containsAll(null, "Joe"), Arrays.asList(null, "Bob", "Joe")));
        assertTrue(eval(Conditions.containsOnly(null, "Joe"), Arrays.asList(null, "Joe")));

        // Containment is only applicable to lists/sets
        assertFalse(eval(Conditions.contains(123), null));
        assertFalse(eval(Conditions.contains(123), "some string"));
        assertFalse(eval(Conditions.contains(123), 123));
        assertFalse(eval(Conditions.contains("key"), ImmutableMap.of("key", "value")));
    }

    @Test
    public void testLike() {
        // For each test below, verify the correct matcher is being used and that the matcher is correct
        // Prefix
        assertTrue(Conditions.like("review:*") instanceof LikeConditionImpl.StartsWith);
        assertTrue(eval(Conditions.like("review:*"), "review:testcustomer"));
        assertTrue(eval(Conditions.like("review:*"), "review:"));
        assertFalse(eval(Conditions.like("review:*"), "reviews:testcustomer"));
        // Suffix
        assertTrue(Conditions.like("*:testcustomer") instanceof LikeConditionImpl.EndsWith);
        assertTrue(eval(Conditions.like("*:testcustomer"), "review:testcustomer"));
        assertTrue(eval(Conditions.like("*:testcustomer"), ":testcustomer"));
        assertFalse(eval(Conditions.like("*:testcustomer"), "review:bestcustomer"));
        // Surrounds
        assertTrue(Conditions.like("widget:*:testcustomer") instanceof LikeConditionImpl.Surrounds);
        assertTrue(eval(Conditions.like("widget:*:testcustomer"), "widget:type:testcustomer"));
        assertTrue(eval(Conditions.like("widget:*:testcustomer"), "widget::testcustomer"));
        assertFalse(eval(Conditions.like("widget:*:testcustomer"), "gidget:type:testcustomer"));
        assertFalse(eval(Conditions.like("widget:*:testcustomer"), "widget:type:bestcustomer"));
        // Contains
        assertTrue(Conditions.like("*:type:*") instanceof LikeConditionImpl.Contains);
        assertTrue(eval(Conditions.like("*:type:*"), "widget:type:testcustomer"));
        assertTrue(eval(Conditions.like("*:type:*"), "widget:type:"));
        assertTrue(eval(Conditions.like("*:type:*"), ":type:testcustomer"));
        assertFalse(eval(Conditions.like("*:type:*"), "complete_non_match"));
        assertFalse(eval(Conditions.like("*:type:*"), "a:ty:pe:b"));
        // Multiple with prefix
        assertTrue(Conditions.like("a*b*c*") instanceof LikeConditionImpl.Complex);
        assertTrue(eval(Conditions.like("a*b*c*"), "a_and_b_and_c_and_d"));
        assertTrue(eval(Conditions.like("a*b*c*"), "abc"));
        assertFalse(eval(Conditions.like("a*b*c*"), "a_and_b"));
        assertFalse(eval(Conditions.like("a*b*c*"), "a_and_c"));
        assertFalse(eval(Conditions.like("a*b*c*"), "f_a_b_c"));
        // Multiple with suffix
        assertTrue(Conditions.like("*a*b*c") instanceof LikeConditionImpl.Complex);
        assertTrue(eval(Conditions.like("*a*b*c"), "1_a_and_b_and_c"));
        assertTrue(eval(Conditions.like("*a*b*c"), "abc"));
        assertFalse(eval(Conditions.like("*a*b*c"), "b_and_c"));
        assertFalse(eval(Conditions.like("*a*b*c"), "a_and_c"));
        assertFalse(eval(Conditions.like("*a*b*c"), "a_b_c_d"));
        // Multiple with prefix and suffix
        assertTrue(Conditions.like("a*b*c") instanceof  LikeConditionImpl.Complex);
        assertTrue(eval(Conditions.like("a*b*c"), "a_or_b_or_c"));
        assertTrue(eval(Conditions.like("a*b*c"), "abc"));
        assertTrue(eval(Conditions.like("a*b*c"), "abcbc"));
        assertFalse(eval(Conditions.like("a*b*c"), "a_c"));
        assertFalse(eval(Conditions.like("a*b*c"), "bc"));
        assertFalse(eval(Conditions.like("a*b*c"), "ab"));
        // Multiple no prefix or suffix
        assertTrue(Conditions.like("*a*b*c*") instanceof LikeConditionImpl.Complex);
        assertTrue(eval(Conditions.like("*a*b*c*"), "qabcq"));
        assertTrue(eval(Conditions.like("*a*b*c*"), "qaabbccq"));
        assertTrue(eval(Conditions.like("*a*b*c*"), "abc"));
        assertFalse(eval(Conditions.like("*a*b*c*"), "acb"));
        assertFalse(eval(Conditions.like("*a*b*c*"), "aaaaaccccc"));
        // Multiple with prefix or suffix
        assertTrue(eval(Conditions.like("a*one*two*"), "a_two_two_one_two_bc"));
        assertFalse(eval(Conditions.like("a*one*two*"), "a_two_two_one_bc"));
        assertTrue(eval(Conditions.like("*c*one*two"), "ab_c_two_two_one_two"));
        assertFalse(eval(Conditions.like("*c*one*two"), "ab_c_two_two_one"));
        // Negative test overlapping substrings
        assertFalse(eval(Conditions.like("baab*baab"), "baab"));
        assertFalse(eval(Conditions.like("abc*cde*efg"), "abcdefg"));
        assertFalse(eval(Conditions.like("*abc*cde*efg*"), "xabcdefgx"));
        // Escape characters
        assertTrue(eval(Conditions.like("wonder\\**man"), "wonder*woman"));
        assertFalse(eval(Conditions.like("wonder\\**man"), "wonderwoman"));
        // Constant (Conditions replaces this with an "equals" condition)
        assertEquals(Conditions.like("cave"), Conditions.equal("cave"));
        assertTrue(LikeConditionImpl.create("cave") instanceof LikeConditionImpl.ExactMatch);
        assertTrue(eval(LikeConditionImpl.create("cave"), "cave"));
        assertFalse(eval(LikeConditionImpl.create("batcave"), "cave"));
        assertFalse(eval(LikeConditionImpl.create("cave"), "batcave"));
        // Only wildcards (Conditions replaces this with an "is(string)" condition
        assertEquals(Conditions.like("*"), Conditions.isString());
        assertTrue(LikeConditionImpl.create("*") instanceof LikeConditionImpl.AnyString);
        assertTrue(eval(LikeConditionImpl.create("*"), "abc"));
        assertTrue(eval(LikeConditionImpl.create("*"), ""));
        assertEquals(Conditions.like("*****"), Conditions.isString());
        assertTrue(LikeConditionImpl.create("*****") instanceof LikeConditionImpl.AnyString);
        assertTrue(eval(LikeConditionImpl.create("*****"), "abc"));
        assertTrue(eval(LikeConditionImpl.create("*****"), ""));
        // Redundant wildcards
        assertTrue(Conditions.like("**:testcustomer") instanceof LikeConditionImpl.EndsWith);
        assertTrue(eval(Conditions.like("**:testcustomer"), "review:testcustomer"));
        assertTrue(Conditions.like("**b***") instanceof LikeConditionImpl.Contains);
        assertTrue(eval(Conditions.like("**b***"), "abc"));
        assertTrue(eval(Conditions.like("**b***"), "b"));
        // Non-string values
        assertFalse(eval(Conditions.like("*"), null));
        assertFalse(eval(Conditions.like("*"), 23));
        assertFalse(eval(Conditions.like("*"), true));
        assertFalse(eval(Conditions.like("*"), ImmutableMap.<String, Object>of("key", "value")));
    }

    @Test
    public void testAndConditionOrdering() {
        // Create an "and" condition with weights out of order
        Condition cheapest = Conditions.mapBuilder()
                .contains("echo", "echo")
                .build();
        Condition middle = Conditions.mapBuilder()
                .contains("how", "now")
                .contains("brown", "cow")
                .build();
        Condition priciest = Conditions.mapBuilder()
                .contains("a", 1)
                .contains("b", 2)
                .contains("c", 3)
                .build();

        Condition andCondition = Conditions.and(middle, priciest, cheapest);

        // Verify condition order doesn't affect equality
        assertEquals(andCondition, Conditions.and(cheapest, middle, priciest));
        assertEquals(andCondition, Conditions.and(priciest, cheapest, middle));
        // Verify condition serializes in original order
        assertEquals(andCondition.toString(), "and({..,\"brown\":\"cow\",\"how\":\"now\"},{..,\"a\":1,\"b\":2,\"c\":3},{..,\"echo\":\"echo\"})");
        // Verify conditions are returned in increasing weight
        Iterator<Condition> conditions = ((AndCondition)andCondition).getConditions().iterator();
        assertEquals(conditions.next(), cheapest);
        assertEquals(conditions.next(), middle);
        assertEquals(conditions.next(), priciest);
        assertFalse(conditions.hasNext());
    }

    @Test
    public void testOrConditionOrdering() {
        // Create an "or" condition with weights out of order
        Condition cheapest = Conditions.mapBuilder()
                .contains("echo", "echo")
                .build();
        Condition middle = Conditions.mapBuilder()
                .contains("how", "now")
                .contains("brown", "cow")
                .build();
        Condition priciest = Conditions.mapBuilder()
                .contains("a", 1)
                .contains("b", 2)
                .contains("c", 3)
                .build();

        Condition orCondition = Conditions.or(middle, priciest, cheapest);

        // Verify condition order doesn't affect equality
        assertEquals(orCondition, Conditions.or(cheapest, middle, priciest));
        assertEquals(orCondition, Conditions.or(priciest, cheapest, middle));
        // Verify condition serializes in original order
        assertEquals(orCondition.toString(), "or({..,\"brown\":\"cow\",\"how\":\"now\"},{..,\"a\":1,\"b\":2,\"c\":3},{..,\"echo\":\"echo\"})");
        // Verify conditions are returned in increasing weight
        Iterator<Condition> conditions = ((OrCondition)orCondition).getConditions().iterator();
        assertEquals(conditions.next(), cheapest);
        assertEquals(conditions.next(), middle);
        assertEquals(conditions.next(), priciest);
        assertFalse(conditions.hasNext());
    }

    @Test
    public void testMapConditionOrdering() {
        // Create a "map" condition with weights out of order
        Condition cheapest = Conditions.mapBuilder()
                .contains("echo", "echo")
                .build();
        Condition middle = Conditions.mapBuilder()
                .contains("how", "now")
                .contains("brown", "cow")
                .build();
        Condition priciest = Conditions.mapBuilder()
                .contains("a", 1)
                .contains("b", 2)
                .contains("c", 3)
                .build();

        Condition mapCondition = Conditions.mapBuilder()
                .matches("k0", priciest)
                .matches("k1", cheapest)
                .matches("k2", middle)
                .build();

        // Verify condition order doesn't affect equality
        assertEquals(mapCondition,
                Conditions.mapBuilder()
                        .matches("k1", cheapest)
                        .matches("k0", priciest)
                        .matches("k2", middle)
                        .build());

        // Verify condition serializes in natural order by key
        assertEquals(mapCondition.toString(), "{..,\"k0\":{..,\"a\":1,\"b\":2,\"c\":3},\"k1\":{..,\"echo\":\"echo\"},\"k2\":{..,\"brown\":\"cow\",\"how\":\"now\"}}");
        // Verify conditions are returned in increasing weight
        Iterator<Map.Entry<String, Condition>> conditions = ((MapCondition)mapCondition).getEntries().entrySet().iterator();
        assertEquals(conditions.next(), Maps.immutableEntry("k1", cheapest));
        assertEquals(conditions.next(), Maps.immutableEntry("k2", middle));
        assertEquals(conditions.next(), Maps.immutableEntry("k0", priciest));
        assertFalse(conditions.hasNext());
    }

    private boolean eval(Condition condition, @Nullable Object root) {
        return eval(condition, root, mock(Intrinsics.class));
    }

    private boolean eval(Condition condition, @Nullable Object root, Intrinsics intrinsics) {
        return ConditionEvaluator.eval(condition, root, intrinsics);
    }
}
