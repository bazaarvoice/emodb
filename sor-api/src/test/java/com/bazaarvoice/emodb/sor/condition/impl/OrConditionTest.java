package com.bazaarvoice.emodb.sor.condition.impl;

import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.condition.State;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Tests various optimizations performed by the OrConditionBuilderImpl build() method.
 */
public class OrConditionTest {

    @Test
    public void testEmpty() {
        Condition or = Conditions.or();

        assertTrue(or instanceof ConstantConditionImpl);
        assertEquals(or, ConstantConditionImpl.FALSE);
    }

    @Test
    public void testSingleton() {
        Condition or = Conditions.or(Conditions.equal(1));

        assertTrue(or instanceof EqualConditionImpl);
        assertEquals(or, new EqualConditionImpl(1));
    }

    @Test
    public void testEqualMultipleValues() {
        Condition or = Conditions.or(Conditions.equal(1), Conditions.equal("hello world"));

        assertTrue(or instanceof InConditionImpl);
        assertEquals(or, new InConditionImpl(ImmutableSet.<Object>of(1, "hello world")));
    }

    @Test
    public void testEqualIntrinsics() {
        Condition or = Conditions.orBuilder().
                or(Conditions.intrinsic(Intrinsic.TABLE, "foo")).
                or(Conditions.intrinsic(Intrinsic.TABLE, "bar")).
                build();

        assertTrue(or instanceof IntrinsicConditionImpl);
        assertEquals(or, new IntrinsicConditionImpl(Intrinsic.TABLE,
                new InConditionImpl(ImmutableSet.<Object>of("bar", "foo"))));
    }

    @Test
    public void testMixed() {
        Condition or = Conditions.orBuilder().
                or(Conditions.equal(1)).
                or(Conditions.equal(5)).
                or(Conditions.intrinsic(Intrinsic.TABLE, "foo")).
                or(Conditions.intrinsic(Intrinsic.ID, "id")).
                or(Conditions.intrinsic(Intrinsic.TABLE, "bar")).
                or(Conditions.is(State.UNDEFINED)).
                build();

        assertTrue(or instanceof OrConditionImpl);
        assertEquals(or, new OrConditionImpl(ImmutableList.<Condition>of(
                new InConditionImpl(ImmutableSet.<Object>of(1, 5)),
                new IntrinsicConditionImpl(Intrinsic.ID, new EqualConditionImpl("id")),
                new IntrinsicConditionImpl(Intrinsic.TABLE, new InConditionImpl(ImmutableSet.<Object>of("bar", "foo"))),
                new IsConditionImpl(State.UNDEFINED))));
    }
}
