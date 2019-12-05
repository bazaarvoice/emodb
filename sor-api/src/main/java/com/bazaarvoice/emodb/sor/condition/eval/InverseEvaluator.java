package com.bazaarvoice.emodb.sor.condition.eval;

import com.bazaarvoice.emodb.sor.condition.AndCondition;
import com.bazaarvoice.emodb.sor.condition.ComparisonCondition;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.ConditionVisitor;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.condition.ConstantCondition;
import com.bazaarvoice.emodb.sor.condition.ContainsCondition;
import com.bazaarvoice.emodb.sor.condition.EqualCondition;
import com.bazaarvoice.emodb.sor.condition.InCondition;
import com.bazaarvoice.emodb.sor.condition.IntrinsicCondition;
import com.bazaarvoice.emodb.sor.condition.IsCondition;
import com.bazaarvoice.emodb.sor.condition.LikeCondition;
import com.bazaarvoice.emodb.sor.condition.MapCondition;
import com.bazaarvoice.emodb.sor.condition.NotCondition;
import com.bazaarvoice.emodb.sor.condition.OrCondition;
import com.bazaarvoice.emodb.sor.condition.PartitionCondition;
import com.bazaarvoice.emodb.sor.condition.State;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Condition visitor to return the inverse of a condition.  If possible this returns an inverse, <code>i</code>,
 * which guarantees that for each value <code>v</code> and intrinsics <code>intr</code>:
 *
 * <code>ConditionEvaluator.eval(x, v, intr) != ConditionEvaluator.eval(i, v, intr)</code>.
 *
 * This evaluator is only used internally by {@link SubsetEvaluator} and is only implemented to the extent that class
 * requires.  Not every condition has a well-defined inversion with the current set of conditions.  For example,
 * there is no inverse for <code>"constant"</code> or <code>like("prefix:*")</code>.  If there is no well-defined
 * condition which inverts the input condition then this evaluator returns null rather than not(condition).
 */
class InverseEvaluator implements ConditionVisitor<Void, Condition> {

    @Nullable
    public static Condition getInverseOf(Condition condition) {
        return checkNotNull(condition, "condition").visit(new InverseEvaluator(), null);
    }

    @Nullable
    @Override
    public Condition visit(ConstantCondition condition, Void context) {
        return Conditions.always(!condition.getValue());
    }

    @Nullable
    @Override
    public Condition visit(NotCondition condition, Void context) {
        return condition.getCondition();
    }

    @Nullable
    @Override
    public Condition visit(IsCondition condition, Void context) {
        switch (condition.getState()) {
            case DEFINED:
                return Conditions.isUndefined();

            case UNDEFINED:
                return Conditions.isDefined();

            default:
                List<Condition> inverseConditions = Lists.newArrayList();
                for (State state : State.values()) {
                    if (state != condition.getState() && state != State.DEFINED) {
                        inverseConditions.add(Conditions.is(state));
                    }
                }
                return Conditions.or(inverseConditions);
        }
    }

    @Nullable
    @Override
    public Condition visit(ComparisonCondition condition, Void context) {
        Condition inverseComparison;
        switch (condition.getComparison()) {
            case GT:
                inverseComparison = Conditions.le(condition.getValue());
                break;

            case GE:
                inverseComparison = Conditions.lt(condition.getValue());
                break;

            case LT:
                inverseComparison = Conditions.ge(condition.getValue());
                break;

            case LE:
                inverseComparison = Conditions.gt(condition.getValue());
                break;

            default:
                // Should be unreachable
                throw new UnsupportedOperationException(condition.getComparison().toString());
        }

        // Because the comparison evaluator returns false if the type doesn't match then the inverse should do
        // the opposite.
        Condition typeCondition;
        if (condition.getValue() instanceof Number) {
            typeCondition = Conditions.isNumber();
        } else if (condition.getValue() instanceof String) {
            typeCondition = Conditions.isString();
        } else {
            // Comparisons should only support numbers and strings
            throw new UnsupportedOperationException("Unsupported comparison type: " + condition.getValue().getClass());
        }

        // IsConditions are invertible, so include the inverse rather than not(string) or not(number)
        Condition notTypeCondition = typeCondition.visit(this, null);

        return Conditions.or(notTypeCondition, inverseComparison);
    }

    @Nullable
    @Override
    public Condition visit(IntrinsicCondition condition, Void context) {
        Condition inverse = condition.getCondition().visit(this, null);
        if (inverse == null) {
            return null;
        }
        return Conditions.intrinsic(condition.getName(), inverse);
    }

    @Nullable
    @Override
    public Condition visit(AndCondition condition, Void context) {
        return inverseConditions(condition.getConditions(), true);
    }

    @Nullable
    @Override
    public Condition visit(OrCondition condition, Void context) {
        return inverseConditions(condition.getConditions(), false);
    }

    @Nullable
    private Condition inverseConditions(Collection<Condition> conditions, boolean fromAnd) {
        // Use DeMorgan's law.  In this case allow "not(sub-condition)" for any sub-conditions which do not have an
        // inverse, leaving future operations to handle if necessary.
        List<Condition> inverseConditions = conditions.stream()
                .map(condition -> MoreObjects.firstNonNull(condition.visit(this, null), Conditions.not(condition)))
                .collect(Collectors.toList());

        if (fromAnd) {
            return Conditions.or(inverseConditions);
        }
        return Conditions.and(inverseConditions);
    }

    @Nullable
    @Override
    public Condition visit(MapCondition condition, Void context) {
        // A map condition is basically an AND of all provided key conditions.  So the inverse is an OR or
        // of the same with each key condition inverted.  As with the AND operation allow "not(sub-condition)"
        // when a sub-condition has no inverse.
        List<Condition> conditions = Lists.newArrayListWithCapacity(condition.getEntries().size());
        for (Map.Entry<String, Condition> entry : condition.getEntries().entrySet()) {
            Condition keyCond = entry.getValue();
            Condition inverted = MoreObjects.firstNonNull(keyCond.visit(this, null), Conditions.not(keyCond));
            conditions.add(Conditions.mapBuilder().matches(entry.getKey(), inverted).build());
        }

        // Since evaluating map conditions returns false if the value is not a map that needs to be inverted as well.
        conditions.add(Conditions.isMap().visit(this, null));

        return Conditions.or(conditions);
    }

    @Nullable
    @Override
    public Condition visit(PartitionCondition condition, Void context) {
        Condition inverse = condition.getCondition().visit(this, null);
        if (inverse == null) {
            return null;
        }
        return Conditions.partition(condition.getNumPartitions(), inverse);
    }

    // The remaining conditions have no well-defined inverse expressible as a Condition other than not(condition).

    @Nullable
    @Override
    public Condition visit(EqualCondition condition, Void context) {
        return null;
    }

    @Nullable
    @Override
    public Condition visit(InCondition condition, Void context) {
        return null;
    }

    @Nullable
    @Override
    public Condition visit(ContainsCondition condition, Void context) {
        return null;
    }

    @Nullable
    @Override
    public Condition visit(LikeCondition condition, Void context) {
        return null;
    }
}
