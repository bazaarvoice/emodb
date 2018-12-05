package com.bazaarvoice.emodb.sor.condition;

import com.bazaarvoice.emodb.sor.condition.impl.AndConditionBuilderImpl;
import com.bazaarvoice.emodb.sor.condition.impl.ComparisonConditionImpl;
import com.bazaarvoice.emodb.sor.condition.impl.ConstantConditionImpl;
import com.bazaarvoice.emodb.sor.condition.impl.EqualConditionImpl;
import com.bazaarvoice.emodb.sor.condition.impl.InConditionImpl;
import com.bazaarvoice.emodb.sor.condition.impl.IntrinsicConditionImpl;
import com.bazaarvoice.emodb.sor.condition.impl.IsConditionImpl;
import com.bazaarvoice.emodb.sor.condition.impl.LikeConditionImpl;
import com.bazaarvoice.emodb.sor.condition.impl.MapConditionBuilderImpl;
import com.bazaarvoice.emodb.sor.condition.impl.NotConditionImpl;
import com.bazaarvoice.emodb.sor.condition.impl.OrConditionBuilderImpl;
import com.bazaarvoice.emodb.sor.condition.impl.PartitionConditionImpl;
import com.bazaarvoice.emodb.sor.delta.deser.DeltaParser;
import com.bazaarvoice.emodb.sor.delta.impl.ContainsConditionImpl;
import com.google.common.collect.Sets;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

public abstract class Conditions {

    public static Condition fromString(String string) {
        return DeltaParser.parseCondition(string);
    }

    public static Condition always(boolean value) {
        return value ? alwaysTrue() : alwaysFalse();
    }

    public static Condition alwaysFalse() {
        return ConstantConditionImpl.FALSE;
    }

    public static Condition alwaysTrue() {
        return ConstantConditionImpl.TRUE;
    }

    public static Condition equal(@Nullable Object json) {
        return new EqualConditionImpl(json);
    }

    public static Condition in(Object... json) {
        return in(Arrays.asList(json));
    }

    public static Condition in(Collection<?> json) {
        if (json.isEmpty()) {
            return alwaysFalse();
        } else if (json.size() == 1) {
            return equal(json.iterator().next());
        }
        Set<Object> set = Sets.newLinkedHashSet(json);
        if (set.size() == 1) {
            return equal(set.iterator().next());
        } else {
            return new InConditionImpl(set);
        }
    }

    public static Condition intrinsic(String name, @Nullable Object json) {
        return intrinsic(name, equal(json));
    }

    public static Condition intrinsic(String name, Condition condition) {
        return new IntrinsicConditionImpl(name, condition);
    }

    public static Condition isUndefined() {
        return is(State.UNDEFINED);
    }

    public static Condition isDefined() {
        return is(State.DEFINED);
    }

    public static Condition isNull() {
        return is(State.NULL);
    }

    public static Condition isBoolean() {
        return is(State.BOOL);
    }

    public static Condition isNumber() {
        return is(State.NUM);
    }

    public static Condition isString() {
        return is(State.STRING);
    }

    public static Condition isList() {
        return is(State.ARRAY);
    }

    public static Condition isMap() {
        return is(State.OBJECT);
    }

    public static Condition is(State state) {
        if (state == State.NULL) {
            return equal(null);  // equivalent to is(State.NULL) but more compact
        }
        return new IsConditionImpl(state);
    }

    public static Condition gt(Object json) {
        return compare(Comparison.GT, json);
    }

    public static Condition ge(Object json) {
        return compare(Comparison.GE, json);
    }

    public static Condition lt(Object json) {
        return compare(Comparison.LT, json);
    }

    public static Condition le(Object json) {
        return compare(Comparison.LE, json);
    }

    public static Condition compare(Comparison comparison, Object json) {
        return new ComparisonConditionImpl(comparison, json);
    }

    public static Condition contains(Object json) {
        return new ContainsConditionImpl(json);
    }

    public static Condition containsAny(Object... json) {
        return containsAny(Arrays.asList(json));
    }

    public static Condition containsAny(Collection<Object> json) {
        return contains(json, ContainsCondition.Containment.ANY);
    }

    public static Condition containsAll(Object... json) {
        return containsAll(Arrays.asList(json));
    }

    public static Condition containsAll(Collection<Object> json) {
        return contains(json, ContainsCondition.Containment.ALL);
    }

    public static Condition containsOnly(Object... json) {
        return containsOnly(Arrays.asList(json));
    }

    public static Condition containsOnly(Collection<Object> json) {
        return contains(json, ContainsCondition.Containment.ONLY);
    }

    private static Condition contains(Collection<Object> json, ContainsCondition.Containment containment) {
        if (json.isEmpty() && containment != ContainsCondition.Containment.ONLY) {
            // Every possible value satisfies the condition containing an empty subset
            return alwaysTrue();
        }
        Set<Object> values = Sets.newLinkedHashSet(json);
        return new ContainsConditionImpl(values, containment);
    }

    public static Condition like(String expression) {
        return LikeConditionImpl.create(expression).simplify();
    }

    public static Condition not(Condition condition) {
        if (condition instanceof NotCondition) {
            return ((NotCondition) condition).getCondition();
        }
        return new NotConditionImpl(condition);
    }

    public static Condition or(Condition... conditions) {
        return or(Arrays.asList(conditions));
    }

    public static Condition or(Collection<? extends Condition> conditions) {
        return new OrConditionBuilderImpl().orAny(conditions).build();
    }

    public static OrConditionBuilder orBuilder() {
        return new OrConditionBuilderImpl();
    }

    public static Condition and(Condition... conditions) {
        return and(Arrays.asList(conditions));
    }

    public static Condition and(Collection<? extends Condition> conditions) {
        return new AndConditionBuilderImpl().andAll(conditions).build();
    }

    public static AndConditionBuilder andBuilder() {
        return new AndConditionBuilderImpl();
    }

    public static MapConditionBuilder mapBuilder() {
        return new MapConditionBuilderImpl();
    }

    public static Condition partition(int numPartitions, int partition) {
        return partition(numPartitions, equal(partition));
    }

    public static Condition partition(int numPartitions, Integer... partitions) {
        return partition(numPartitions, in(Arrays.asList(partitions)));
    }

    public static Condition partition(int numPartitions, Condition condition) {
        return new PartitionConditionImpl(numPartitions, condition);
    }
}
