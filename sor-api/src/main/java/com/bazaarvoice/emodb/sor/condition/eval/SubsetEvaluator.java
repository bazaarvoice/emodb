package com.bazaarvoice.emodb.sor.condition.eval;

import com.bazaarvoice.emodb.sor.condition.AndCondition;
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
import com.bazaarvoice.emodb.sor.condition.PartitionCondition;
import com.bazaarvoice.emodb.sor.condition.State;
import com.google.common.base.Objects;
import com.google.common.collect.Sets;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Evaluator for determining if, for two conditions left and right, left is a subset of right.  This relationship
 * is true if for every input v for which <code>ConditionEvaluator.evaluate(left, v, intrinsics)</code> returns
 * true <code>ConditionEvaluator.evaluate(right, v, intrinsics)</code> also returns true.
 *
 * For example:
 *
 * <code>
 *     SubsetEvaluator.isSubset(Conditions.equal("review"), Conditions.in("review", "question")) == true
 *     SubsetEvaluator.isSubset(Conditions.equal("app_global:sys"), Conditions.like("*:ugc")) == false
 * </code>
 */
public class SubsetEvaluator implements ConditionVisitor<Condition, Boolean> {

    public static boolean isSubset(Condition left, Condition right) {
        return new SubsetEvaluator().checkIsSubset(requireNonNull(left, "left"), requireNonNull(right, "right"));
    }

    private boolean checkIsSubset(Condition left, Condition right) {
        return left.visit(this, right);
    }

    @Override
    public Boolean visit(ConstantCondition left, Condition right) {
        if (!left.getValue()) {
            // Always false is a subset of everything
            return true;
        }
        LeftResolvedVisitor<ConstantCondition> visitor = new LeftResolvedVisitor<>(left);
        return right.visit(visitor, null);
    }

    @Override
    public Boolean visit(EqualCondition left, Condition right) {
        LeftResolvedVisitor<EqualCondition> visitor = new LeftResolvedVisitor<EqualCondition>(left) {
            @Override
            public Boolean visit(EqualCondition right, Void context) {
                // Example: "value" subset? "value"
                return Objects.equal(_left.getValue(), right.getValue());
            }

            @Override
            public Boolean visit(InCondition right, Void context) {
                // Example: "do" subset? in("do", "re", "mi")
                return ConditionEvaluator.eval(right, _left.getValue(), null);
            }

            @Override
            public Boolean visit(IsCondition right, Void context) {
                // Example: "value" subset? is(string)
                return right.getState() == State.DEFINED || ConditionEvaluator.eval(right, _left.getValue(), null);
            }

            @Override
            public Boolean visit(ContainsCondition right, Void context) {
                // Example: ["value"] subset? contains("value")
                return ConditionEvaluator.eval(right, left.getValue(), null);
            }

            @Override
            public Boolean visit(ComparisonCondition right, Void context) {
                // Example: "value" subset? gt("v")
                return ConditionEvaluator.eval(right, left.getValue(), null);
            }

            @Override
            public Boolean visit(LikeCondition right, Void context) {
                // Example: "value" subset? like("val*")
                return ConditionEvaluator.eval(right, left.getValue(), null);
            }

            @Override
            public Boolean visit(MapCondition condition, Void context) {
                // Example: {"key":"value"} subset? {..,"key":like("v*")}
                return ConditionEvaluator.eval(right, _left.getValue(), null);
            }
        };

        return right.visit(visitor, null);
    }

    @Override
    public Boolean visit(InCondition left, Condition right) {
        LeftResolvedVisitor<InCondition> visitor = new LeftResolvedVisitor<InCondition>(left) {
            @Override
            public Boolean visit(InCondition right, Void context) {
                // Example: in("car", truck") subset? in("car", "truck", "boat")
                return right.getValues().containsAll(_left.getValues());
            }

            @Override
            public Boolean visit(EqualCondition right, Void context) {
                // Example: in("value") subset? "value"
                return _left.getValues().equals(Sets.newHashSet(right.getValue()));
            }

            @Override
            public Boolean visit(IsCondition right, Void context) {
                // Example: in("car", "truck") subset? is(string)
                return right.getState() == State.DEFINED ||
                        _left.getValues().stream().allMatch((value) -> ConditionEvaluator.eval(right, value, null));
            }

            @Override
            public Boolean visit(ComparisonCondition right, Void context) {
                // Example: in(10, 20) subset? gt(0)
                return _left.getValues().stream().allMatch((value) -> ConditionEvaluator.eval(right, value, null));
            }

            @Override
            public Boolean visit(ContainsCondition right, Void context) {
                // Example: in(["car", "truck"], ["car", boat"]) subset? contains("car")
                return _left.getValues().stream().allMatch((value) -> ConditionEvaluator.eval(right, value, null));
            }

            @Override
            public Boolean visit(LikeCondition right, Void context) {
                // Example: in("car", "truck") subset? like("*c*")
                return _left.getValues().stream().allMatch((value) -> ConditionEvaluator.eval(right, value, null));
            }

            @Override
            public Boolean visit(MapCondition condition, Void context) {
                // Example: in({"key":"car"},{"key":"truck"}) subset? {..,"key":in("car", "truck")}
                return _left.getValues().stream().allMatch((value) -> ConditionEvaluator.eval(right, value, null));
            }
        };

        return right.visit(visitor, null);
    }

    @Override
    public Boolean visit(IntrinsicCondition left, Condition right) {
        LeftResolvedVisitor<IntrinsicCondition> visitor = new LeftResolvedVisitor<IntrinsicCondition>(left) {
            @Override
            public Boolean visit(IntrinsicCondition right, Void context) {
                // Example: intrinsic("~table": "value") subset? intrinsic("~table": like("v*"))
                return _left.getName().equals(right.getName()) &&
                        checkIsSubset(left.getCondition(), right.getCondition());
            }
        };

        return right.visit(visitor, null);
    }

    @Override
    public Boolean visit(IsCondition left, Condition right) {
        LeftResolvedVisitor<IsCondition> visitor = new LeftResolvedVisitor<IsCondition>(left) {
            @Override
            public Boolean visit(IsCondition right, Void context) {
                // Example: is(string) subset? is(defined)
                if (right.getState() == State.DEFINED) {
                    return _left.getState() != State.UNDEFINED;
                }
                return _left.getState() == right.getState();
            }
        };

        return right.visit(visitor, null);
    }

    @Override
    public Boolean visit(ComparisonCondition left, Condition right) {
        LeftResolvedVisitor<ComparisonCondition> visitor = new LeftResolvedVisitor<ComparisonCondition>(left) {
            @Override
            public Boolean visit(IsCondition right, Void context) {
                // Example: gt(5) subset? is(number)
                if (right.getState() == State.DEFINED) {
                    return true;
                }
                return (_left.getValue() instanceof String && right.getState() == State.STRING) ||
                        (_left.getValue() instanceof Number && right.getState() == State.NUM);
            }

            @Override
            public Boolean visit(ComparisonCondition right, Void context) {
                // Example: gt(5) subset? gt(3)
                return _left.isSubsetOf(right);
            }
        };

        return right.visit(visitor, null);
    }

    @Override
    public Boolean visit(ContainsCondition left, Condition right) {
        LeftResolvedVisitor<ContainsCondition> visitor = new LeftResolvedVisitor<ContainsCondition>(left) {
            @Override
            public Boolean visit(IsCondition right, Void context) {
                // Example: contains("value") subset? is(array)
                return right.getState() == State.DEFINED || right.getState() == State.ARRAY;
            }

            @Override
            public Boolean visit(ContainsCondition right, Void context) {
                // Example: containsAll("value1", "value2") subset? contains("value1")
                return left.isSubsetOf(right);
            }
        };

        return right.visit(visitor, null);
    }

    @Override
    public Boolean visit(LikeCondition left, Condition right) {
        LeftResolvedVisitor<LikeCondition> visitor = new LeftResolvedVisitor<LikeCondition>(left) {
            @Override
            public Boolean visit(IsCondition right, Void context) {
                // Example: like("value*") subset? is(string)
                return right.getState() == State.DEFINED || right.getState() == State.STRING;
            }

            @Override
            public Boolean visit(EqualCondition right, Void context) {
                // Example: like("value") subset? "value"
                Object value = right.getValue();
                if (value != null && value instanceof String) {
                    String rValue = (String) value;
                    String lValue = _left.getCondition();
                    // If it contains any wildcards it can't be a subset
                    if (lValue.matches(".*(?<!\\\\)\\*.*")) {
                        return false;
                    }
                    // Unescape all slashes
                    lValue = lValue.replaceAll("\\\\\\\\", "\\\\");
                    return lValue.equals(rValue);
                }
                return false;
            }

            @Override
            public Boolean visit(ComparisonCondition right, Void context) {
                // Example: like("v*") subset? gt("v")
                if (right.getValue() instanceof String) {
                    String value = (String) right.getValue();
                    // If the condition has a prefix then it can be used for comparison.

                    String prefix = _left.getPrefix();
                    if (prefix == null) {
                        // The condition is something like "*abc", so it cannot be definitively bound for comparison.
                        return false;
                    }

                    return prefix.length() >= value.length() && ConditionEvaluator.eval(right, prefix, null);
                }
                return false;
            }

            @Override
            public Boolean visit(LikeCondition right, Void context) {
                // Example: like("val*ue") subset? like("v*e")
                return _left.isSubsetOf(right);
            }
        };

        return right.visit(visitor, null);
    }

    @Override
    public Boolean visit(MapCondition left, Condition right) {
        LeftResolvedVisitor<MapCondition> visitor = new LeftResolvedVisitor<MapCondition>(left) {
            @Override
            public Boolean visit(IsCondition right, Void context) {
                // Example: {..,"key": "value"} subset? is(map)
                return right.getState() == State.DEFINED || right.getState() == State.OBJECT;
            }

            @Override
            public Boolean visit(MapCondition right, Void context) {
                // Example: {..,"key": "value"} subset? {..,"key": in("value", "other")}

                // The conditions for a map condition to be a subset of another map condition are:
                // 1. All keys in the right condition must appear in the left condition.
                // 2. Each left key's condition must be a subset of the matching right's condition.

                if (!_left.getEntries().keySet().containsAll(right.getEntries().keySet())) {
                    return false;
                }

                for (Map.Entry<String, Condition> entry : right.getEntries().entrySet()) {
                    Condition lCondition = _left.getEntries().get(entry.getKey());
                    Condition rCondition = entry.getValue();

                    if (!checkIsSubset(lCondition, rCondition)) {
                        return false;
                    }
                }

                return true;
            }
        };

        return right.visit(visitor, null);
    }

    @Override
    public Boolean visit(NotCondition left, Condition right) {
        // If the left has an inverse then use that
        Condition inverseLeft = InverseEvaluator.getInverseOf(left.getCondition());
        if (inverseLeft != null) {
            return checkIsSubset(inverseLeft, right);
        }

        // Default visitor behavior will evaluate this correctly, returning false in all cases except alwaysTrue()
        // and the double-negative condition, not(right) subset? not(left)
        LeftResolvedVisitor<NotCondition> visitor = new LeftResolvedVisitor<>(left);
        return right.visit(visitor, null);
    }

    @Override
    public Boolean visit(AndCondition left, Condition right) {
        if (!(right instanceof AndCondition)) {
            // Example: and(gt("a"),lt("e")) subset? gt("a")

            // Right must be satisfied by at least one "and" condition
            for (Condition andCondition : left.getConditions()) {
                if (checkIsSubset(andCondition, right)) {
                    return true;
                }
            }
            return false;
        }

        // Example: and({..,"key":"value"},intrinsic("~table":"ugc_us:ugc")) subset? and({..,"key":like("v*")},intrinsic("~table":like("*:ugc"))

        // Each right condition must have at least one left condition that is a subset of it

        for (Condition rightCondition : ((AndCondition) right).getConditions()) {
            if (!left.getConditions().stream().anyMatch(leftCondition -> checkIsSubset(leftCondition, rightCondition))) {
                return false;
            }
        }

        return true;
    }

    @Override
    public Boolean visit(OrCondition left, Condition right) {
        // Example: or(gt("e"),in("a","c")) subset? is(string)
        for (Condition orCondition : left.getConditions()) {
            if (!checkIsSubset(orCondition, right)) {
                return false;
            }
        }
        return true;
    }


    @Override
    public Boolean visit(PartitionCondition left, Condition right) {
        LeftResolvedVisitor<PartitionCondition> visitor = new LeftResolvedVisitor<PartitionCondition>(left) {
            @Override
            public Boolean visit(PartitionCondition right, Void context) {
                // Example: partition(8:1) subset? partition(8:in(1,2))
                return _left.getNumPartitions() == right.getNumPartitions() &&
                        checkIsSubset(left.getCondition(), right.getCondition());
            }
        };

        return right.visit(visitor, null);
    }

    /**
     * Visitor used to provide typed evaluation of both the left and right conditions.  Most implementations
     * leave it up to the caller to define behavior, but implementations for the common operations "and", "or",
     * "not", and "alwaysX()" are provided.
     */
    private class LeftResolvedVisitor<T extends Condition> implements ConditionVisitor<Void, Boolean> {

        T _left;

        private LeftResolvedVisitor(T left) {
            _left = left;
        }

        @Override
        public final Boolean visit(ConstantCondition right, Void context) {
            // Examples: _left subset? alwaysTrue()
            //           _left subset? alwaysFalse()
            if (right.getValue()) {
                // Every condition is a subset of alwaysTrue()
                return true;
            }
            // Only alwaysFalse() is a subset of alwaysFalse().  There are infinitely more contrived ones,
            // such as and(eq("foo"),eq("bar")), but for simplicity only return true for alwaysFalse().
            return _left instanceof ConstantCondition && !((ConstantCondition) _left).getValue();
        }

        @Override
        public final Boolean visit(AndCondition right, Void context) {
            // left is a subset of right only if left is a subset of each "and" condition
            for (Condition andCondition : right.getConditions()) {
                if (!andCondition.visit(this, context)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public final Boolean visit(OrCondition right, Void context) {
            // left is a subset of right only if left is a subset of at least one "or" condition
            for (Condition orCondition : right.getConditions()) {
                if (orCondition.visit(this, context)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public final Boolean visit(NotCondition right, Void context) {
            // If possible invert the right condition
            Condition inverseRight = InverseEvaluator.getInverseOf(right.getCondition());
            if (inverseRight != null) {
                return checkIsSubset(_left, inverseRight);
            }

            // If possible invert the left condition. Since "not(x) subset not(y)" == "y subset x", if the left condition
            // can be inverted such that "not(invertedLeft)" == "left" then we can return  "right.condition subset inverseLeft"
            Condition inverseLeft = InverseEvaluator.getInverseOf(_left);
            if (inverseLeft != null) {
                return checkIsSubset(right.getCondition(), inverseLeft);
            }

            // Neither left nor right have an inverse.  If the right's condition is distinct from left then it is
            // a subset.  For example:  "ugc_us:ugc" is a subset of "not(like("*:sys"))" because "ugc_us:ugc"
            // and "like("*:sys")" are distinct.

            return DistinctEvaluator.areDistinct(_left, right.getCondition());
        }

        // All remaining methods return false be default.  Subclasses should override those methods whose return
        // values may be true.

        @Override
        public Boolean visit(EqualCondition right, Void context) {
            return false;
        }

        @Override
        public Boolean visit(InCondition right, Void context) {
            return false;
        }

        @Override
        public Boolean visit(IntrinsicCondition right, Void context) {
            return false;
        }

        @Override
        public Boolean visit(IsCondition right, Void context) {
            return false;
        }

        @Override
        public Boolean visit(ComparisonCondition right, Void context) {
            return false;
        }

        @Override
        public Boolean visit(ContainsCondition right, Void context) {
            return false;
        }

        @Override
        public Boolean visit(LikeCondition right, Void context) {
            return false;
        }

        @Override
        public Boolean visit(MapCondition right, Void context) {
            return false;
        }

        @Override
        public Boolean visit(PartitionCondition condition, Void context) {
            return false;
        }
    }
}
