package com.bazaarvoice.emodb.sor.condition.eval;

import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.condition.AndCondition;
import com.bazaarvoice.emodb.sor.condition.Comparison;
import com.bazaarvoice.emodb.sor.condition.ComparisonCondition;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.ConditionVisitor;
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
import com.bazaarvoice.emodb.sor.condition.State;
import com.bazaarvoice.emodb.sor.delta.eval.DeltaEvaluator;
import com.bazaarvoice.emodb.sor.delta.eval.Intrinsics;
import com.google.common.base.Objects;
import com.google.common.collect.Sets;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class ConditionEvaluator implements ConditionVisitor<Object, Boolean> {

    private final Intrinsics _intrinsics;

    public ConditionEvaluator(@Nullable Intrinsics intrinsics) {
        _intrinsics = intrinsics;
    }

    public static boolean eval(Condition condition, @Nullable Object json, @Nullable Intrinsics intrinsics) {
        return condition.visit(new ConditionEvaluator(intrinsics), json);
    }

    @Override
    public Boolean visit(ConstantCondition condition, @Nullable Object json) {
        return condition.getValue();
    }

    @Override
    public Boolean visit(EqualCondition condition, @Nullable Object json) {
        return Objects.equal(condition.getValue(), json);
    }

    @Override
    public Boolean visit(InCondition condition, @Nullable Object json) {
        return condition.getValues().contains(json);
    }

    @Override
    public Boolean visit(IntrinsicCondition condition, @Nullable Object ignoredJson) {
        return condition.getCondition().visit(this, getIntrinsicValue(condition.getName()));
    }

    private Object getIntrinsicValue(String name) {
        Intrinsics intrinsics = checkNotNull(_intrinsics, "May not reference intrinsic values from this context.");
        if (Intrinsic.ID.equals(name)) {
            return intrinsics.getId();
        } else if (Intrinsic.TABLE.equals(name)) {
            return intrinsics.getTable();
        } else if (Intrinsic.SIGNATURE.equals(name)) {
            return intrinsics.getSignature();
        } else if (Intrinsic.VERSION.equals(name)) {
            // Intrinsic.VERSION is not supported, by design.  It is not reliable with weak consistency (ie. with
            // non-quorum reads or cross-data center.)
            throw new UnsupportedOperationException();
        } else if (Intrinsic.DELETED.equals(name)) {
            return intrinsics.isDeleted();
        } else if (Intrinsic.FIRST_UPDATE_AT.equals(name)) {
            return intrinsics.getFirstUpdateAt();
        } else if (Intrinsic.LAST_UPDATE_AT.equals(name)) {
            return intrinsics.getLastUpdateAt();
        } else if (Intrinsic.LAST_MUTATE_AT.equals(name)) {
            return intrinsics.getLastMutateAt();
        } else if (Intrinsic.PLACEMENT.equals(name)) {
            return intrinsics.getTablePlacement();
        } else {
            throw new UnsupportedOperationException(name);
        }
    }

    @Override
    public Boolean visit(IsCondition condition, @Nullable Object json) {
        State state = condition.getState();
        switch (state) {
            case UNDEFINED:
                return json == DeltaEvaluator.UNDEFINED;
            case DEFINED:
                return json != DeltaEvaluator.UNDEFINED;
            case NULL:
                return json == null;
            case BOOL:
                return json instanceof Boolean;
            case NUM:
                return json instanceof Number;
            case STRING:
                return json instanceof String;
            case ARRAY:
                return json instanceof List;
            case OBJECT:
                return json instanceof Map;
            default:
                throw new UnsupportedOperationException(state.name());
        }
    }

    @Nullable
    @Override
    public Boolean visit(ComparisonCondition condition, @Nullable Object json) {
        Comparison comparison = condition.getComparison();
        Object value = condition.getValue();

        // Null comparisons are always false.
        if (json == null || value == null) {
            return false;
        }
        if (json instanceof Number && value instanceof Number) {
            Number nLeft = (Number) json;
            Number nRight = (Number) value;
            if (promoteToDouble(nLeft) || promoteToDouble(nRight)) {
                return matchesComparison(comparison, Doubles.compare(nLeft.doubleValue(), nRight.doubleValue()));
            } else {
                return matchesComparison(comparison, Longs.compare(nLeft.longValue(), nRight.longValue()));
            }
        }
        if (json instanceof String && value instanceof String) {
            return matchesComparison(comparison, ((String) json).compareTo((String) value));
        }
        // Everything else is unsupported and therefore does not match.
        return false;
    }

    private boolean promoteToDouble(Number number) {
        return number instanceof Float || number instanceof Double;
    }

    private boolean matchesComparison(Comparison comparison, int result) {
        switch (comparison) {
            case LE:
                return result <= 0;
            case LT:
                return result < 0;
            case GE:
                return result >= 0;
            case GT:
                return result > 0;
            default:
                throw new UnsupportedOperationException(String.valueOf(comparison));
        }
    }

    @Nullable
    @Override
    public Boolean visit(ContainsCondition condition, @Nullable Object json) {
        Set<Object> conditionValues = condition.getValues();
        ContainsCondition.Containment containment = condition.getContainment();

        if (conditionValues.isEmpty() && containment != ContainsCondition.Containment.ONLY) {
            // All values satisfy the empty set of conditions on subset containment
            return true;
        }

        boolean isSetType = false;
        if (!(json instanceof List || (isSetType = json instanceof Set))) {
            // Value is not a list or a set
            return false;
        }

        Collection<?> values = (Collection<?>) json;
        // If we're going to traverse the values more than once then convert it into a set
        // Skip if it is already a Set type
        if (!isSetType && (values.size() > 1 || conditionValues.size() > 1)) {
            values = Sets.newHashSet(values);
        }

        boolean isAny = containment == ContainsCondition.Containment.ANY;

        for (Object conditionValue : conditionValues) {
            if (values.contains(conditionValue)) {
                if (isAny) {
                    // Condition is met if any value is contained, which one was
                    return true;
                }
            } else if (!isAny) {
                // Condition is not met if any value is not contained, which one wasn't
                return false;
            }
        }

        // This statement is only reachable if isAny == true and no values were contained OR
        // isAny == false and all values were contained

        if (containment == ContainsCondition.Containment.ONLY) {
            // All values were found; return true only if there were no additional values
            return conditionValues.size() == values.size();
        }

        return !isAny;
    }

    @Nullable
    @Override
    public Boolean visit(LikeCondition condition, @Nullable Object json) {
        // Null values and non-strings always don't match
        return json != null &&
                json instanceof String &&
                condition.matches(json.toString());
    }

    @Override
    public Boolean visit(NotCondition condition, @Nullable Object json) {
        return !condition.getCondition().visit(this, json);
    }

    @Override
    public Boolean visit(AndCondition condition, @Nullable Object json) {
        for (Condition clause : condition.getConditions()) {
            if (!clause.visit(this, json)) {
                return false;
            }
        }
        return true;  // Note: if the condition lists is empty then AND returns true
    }

    @Override
    public Boolean visit(OrCondition condition, @Nullable Object json) {
        for (Condition clause : condition.getConditions()) {
            if (clause.visit(this, json)) {
                return true;
            }
        }
        return false;  // Note: if the condition lists is empty then OR returns false
    }

    @Override
    public Boolean visit(MapCondition condition, @Nullable Object json) {
        if (!(json instanceof Map)) {
            return false;
        }
        Map<?, ?> map = (Map<?, ?>) json;
        for (Map.Entry<String, Condition> entry : condition.getEntries().entrySet()) {
            if (!entry.getValue().visit(this, get(map, entry.getKey()))) {
                return false;
            }
        }
        return true;
    }

    private Object get(Map<?, ?> map, String key) {
        Object value = map.get(key);
        if (value == null && !map.containsKey(key)) {
            value = DeltaEvaluator.UNDEFINED;
        }
        return value;
    }
}
