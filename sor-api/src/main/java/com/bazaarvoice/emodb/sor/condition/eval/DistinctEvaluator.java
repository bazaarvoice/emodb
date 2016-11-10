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
import com.bazaarvoice.emodb.sor.condition.State;
import com.bazaarvoice.emodb.sor.delta.eval.Intrinsics;
import com.google.common.collect.Sets;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Partial implementation of a condition evaluator to determine if two conditions are distinct.  This is true if for all
 * input values at most one of the conditions returns true from {@link ConditionEvaluator#eval(Condition, Object, Intrinsics)}.
 *
 * This evaluator is only used internally by {@link SubsetEvaluator} for the specific case where it is trying to
 * determine if, for two conditions c1 and c2, <code>c1</code> is a subset of <code>not(c2)</code>.  As previously
 * stated this is only a partial implementation and may not return correct values if either the left or right input
 * to {@link #areDistinct(Condition, Condition)} is a {@link NotCondition}, {@link AndCondition}, or {@link OrCondition}.
 * There are two reasons why these implementations are not provided:
 *
 * <ol>
 *     <li>
 *         The implementation for these conditions is complicated and requires condition evaluation capabilities currently
 *         not available.  For example, <code>and(ge(0),le(20))</code> is not distinct with <code>and(ge(10),le(30))</code>
 *         but there is no current ability to combine the two conditions into a single ranged condition.
 *     </li>
 *     <li>
 *         Implementations for these conditions are not necessary because they will never be called from SubsetEvaluator.
 *     </li>
 * </ol>
 *
 * Proof for the second reason above:
 *
 * The only circumstance this evaluator is used is if the right condition in the subset check is a "not" condition
 * and is handled by {@link SubsetEvaluator.LeftResolvedVisitor#visit(NotCondition, Void)}.  That method only checks
 * whether two conditions are distinct if neither the left nor the right's nested condition have an inverse.  "Not", "and", and
 * "or" conditions all have inverses as implemented by {@link InverseEvaluator#visit(NotCondition, Void)},
 * {@link InverseEvaluator#visit(AndCondition, Void)}, and {@link InverseEvaluator#visit(OrCondition, Void)},
 * respectively.  Therefore, SubsetEvaluator will never make a distinct check for any conditions where either the
 * left or right parameter is a "not", "and", or "or" condition.  This implementation returns false in all of those
 * cases as a conservative default, although as proven here those calls are never actually made.
 */
class DistinctEvaluator implements ConditionVisitor<Condition, Boolean> {

    /**
     * For two conditions c1 and c2, <code>areDistinct(c1, c2)</code> == <code>areDistinct(c2, c1)</code>.
     * Rather than implementing every possible combination of left and right condition type, resulting in a good deal
     * of code duplication, this enumeration is used to control which methods actually need to be implemented.  Any call
     * made where the right parameter's order is greater than the left's parameter order calls back to the method with the
     * parameters swapped.
     *
     * For example, a call to <code>visit(IsCondition, LikeCondition)</code> is automatically forwarded to
     * <code>visit(LikeCondition, IsCondition)</code> since <code>ParameterOrder.LIKE <= ParameterOrder.IS</code>.
     */
    private enum ParameterOrder {
        CONSTANT, LIKE, COMPARISON, IS, EQUAL, IN, CONTAINS, MAP, INTRINSIC
    }
    
    static boolean areDistinct(Condition left, Condition right) {
        return new DistinctEvaluator().checkAreDistinct(checkNotNull(left, "left"), checkNotNull(right, "right"));
    }

    private boolean checkAreDistinct(Condition left, Condition right) {
        return left.visit(this, right);
    }

    @Override
    public Boolean visit(ConstantCondition left, Condition right) {
        if (!left.getValue()) {
            // alwaysFalse() is always distinct from any other condition since it never returns true
            return true;
        }

        LeftResolvedVisitor<ConstantCondition> visitor = new LeftResolvedVisitor<ConstantCondition>(left) {
            @Override
            protected boolean visit(ConstantCondition right) {
                // We've already established that _left.getValue() == true, so by the same logic as above
                // they are only distinct if right == alwaysFalse()
                return !right.getValue();
            }
        };

        return right.visit(visitor, ParameterOrder.CONSTANT);
    }

    @Override
    public Boolean visit(LikeCondition left, Condition right) {
        LeftResolvedVisitor<LikeCondition> visitor = new LeftResolvedVisitor<LikeCondition>(left) {
            @Override
            public boolean visit(IsCondition right) {
                // "Like" conditions are true only for strings, so it is distinct if the "is" condition cannot match strings.
                return right.getState() != State.DEFINED && right.getState() != State.STRING;
            }

            @Override
            public boolean visit(EqualCondition right) {
                // "Like" conditions are true only for strings, so it is distinct if the "equal" condition does not
                // contain a string constant which matches the "like" condition.
                return !(right.getValue() instanceof String && _left.matches((String) right.getValue()));
            }

            @Override
            public boolean visit(InCondition right) {
                // The conditions are distinct if the "in" condition contains no values the match the "like" condition.
                for (Object value : right.getValues()) {
                    if (value instanceof String && _left.matches((String) value)) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public boolean visit(ComparisonCondition right) {
                // Numeric comparisons such as gt(10) are always distinct from a "like" condition which only matches strings.
                if (!(right.getValue() instanceof String)) {
                    return true;
                }

                // If the condition has a prefix then it can be used for comparison.
                String prefix = _left.getPrefix();
                if (prefix == null) {
                    // The condition is something like "*abc", so it cannot be distinct from a comparison.
                    return false;
                }

                // The two are distinct if the "like" conditions prefix can never overlap with the comparison.
                // For example, like("ba*") is distinct from ge("be") but not gt("b"), since "bat" would satisfy both.
                String comparisonValue = (String) right.getValue();
                return prefix.length() >= comparisonValue.length() && !ConditionEvaluator.eval(right, prefix, null);
            }

            @Override
            public boolean visit(LikeCondition right) {
                return !_left.overlaps(right);
            }
        };

        return right.visit(visitor, ParameterOrder.LIKE);
    }

    @Override
    public Boolean visit(ComparisonCondition left, Condition right) {
        LeftResolvedVisitor<ComparisonCondition> visitor = new LeftResolvedVisitor<ComparisonCondition>(left) {
            @Override
            public boolean visit(ComparisonCondition right) {
                return !_left.overlaps(right);
            }

            @Override
            protected boolean visit(InCondition right) {
                // If any value in the "in" condition matches then they are not distinct
                for (Object value : right.getValues()) {
                    if (!checkAreDistinct(_left, Conditions.equal(value))) {
                        return false;
                    }
                }
                return true;
            }
        };

        return right.visit(visitor, ParameterOrder.COMPARISON);
    }

    @Override
    public Boolean visit(IsCondition left, Condition right) {
        LeftResolvedVisitor<IsCondition> visitor = new LeftResolvedVisitor<IsCondition>(left) {
            @Override
            protected boolean visit(InCondition right) {
                // If any value in the "in" condition matches then they are not distinct
                for (Object value : right.getValues()) {
                    if (!checkAreDistinct(_left, Conditions.equal(value))) {
                        return false;
                    }
                }
                return true;
            }
        };

        return right.visit(visitor, ParameterOrder.IS);
    }

    @Override
    public Boolean visit(InCondition left, Condition right) {
        LeftResolvedVisitor<InCondition> visitor = new LeftResolvedVisitor<InCondition>(left) {
            @Override
            protected boolean visit(ContainsCondition right) {
                // If any value in the "in" condition matches then they are not distinct
                for (Object value : _left.getValues()) {
                    if (!checkAreDistinct(right, Conditions.equal(value))) {
                        return false;
                    }
                }
                return true;
            }
        };

        return right.visit(visitor, ParameterOrder.IN);
    }

    @Override
    public Boolean visit(IntrinsicCondition left, Condition right) {
        LeftResolvedVisitor<IntrinsicCondition> visitor = new LeftResolvedVisitor<IntrinsicCondition>(left) {
            @Override
            protected boolean visit(IntrinsicCondition right) {
                // The two are distinct only if they match on intrinsic type and the conditions are distinct.
                return _left.getName().equals(right.getName()) && checkAreDistinct(_left.getCondition(), right.getCondition());
            }
        };

        return right.visit(visitor, ParameterOrder.INTRINSIC);
    }

    @Override
    public Boolean visit(ContainsCondition left, Condition right) {
        LeftResolvedVisitor<ContainsCondition> visitor = new LeftResolvedVisitor<ContainsCondition>(left) {
            @Override
            protected boolean visit(ContainsCondition right) {
                return !_left.overlaps(right);
            }
        };

        return right.visit(visitor, ParameterOrder.CONTAINS);
    }

    @Override
    public Boolean visit(MapCondition left, Condition right) {
        // Two maps can only be distinct if at least one key is guaranteed to be distinct from the other
        LeftResolvedVisitor<MapCondition> visitor = new LeftResolvedVisitor<MapCondition>(left) {
            @Override
            protected boolean visit(MapCondition right) {
                Map<String, Condition> leftMap = _left.getEntries();
                Map<String, Condition> rightMap = right.getEntries();
                for (String key : Sets.intersection(leftMap.keySet(), rightMap.keySet())) {
                    if (checkAreDistinct(leftMap.get(key), rightMap.get(key))) {
                        return true;
                    }
                }
                return false;
            }
        };

        return right.visit(visitor, ParameterOrder.MAP);
    }

    @Override
    public Boolean visit(EqualCondition left, Condition right) {
        // There are no overrides necessary for an equality condition; all special cases are handled by other overrides
        // whose order in ParameterOrder will evaluate them in favor of this method.
        return right.visit(new LeftResolvedVisitor<>(left), ParameterOrder.EQUAL);
    }

    /**
     * Future:  Build an actual implementation.  Currently this call is never made and is here only to satisfy
     * the {@link ConditionVisitor} interface.
     */
    @Override
    public Boolean visit(AndCondition left, Condition right) {
        return false;
    }

    /**
     * Future:  Build an actual implementation.  Currently this call is never made and is here only to satisfy
     * the {@link ConditionVisitor} interface.
     */
    @Override
    public Boolean visit(NotCondition left, Condition right) {
        return false;
    }

    /**
     * Future:  Build an actual implementation.  Currently this call is never made and is here only to satisfy
     * the {@link ConditionVisitor} interface.
     */
    @Override
    public Boolean visit(OrCondition left, Condition right) {
        return false;
    }

    /**
     * Visitor used to provide typed evaluation of both the left and right conditions.  Default behavior for all
     * combinations of conditions is:
     *
     * !left.subset(right) && !right.subset(left)
     *
     * For the cases where this is insufficient the subclass should override.
     */
    private class LeftResolvedVisitor<T extends Condition> implements ConditionVisitor<ParameterOrder, Boolean> {

        T _left;

        private LeftResolvedVisitor(T left) {
            _left = left;
        }

        /**
         * Check for whether, based on the left and right parameter type, the order should be reversed to
         * satisfy requiring only a single implementation.
         */
        private boolean swapParameters(ParameterOrder leftOrder, ParameterOrder rightType) {
            return rightType.compareTo(leftOrder) < 0;
        }

        /**
         * Returns the default distinct check which works most commonly, where neither left nor right are
         * a subset of the other.
         */
        boolean defaultDistinctCheck(Condition right) {
            return !SubsetEvaluator.isSubset(_left, right) && !SubsetEvaluator.isSubset(right, _left);
        }

        @Override
        public final Boolean visit(ConstantCondition right, ParameterOrder leftOrder) {
            if (swapParameters(leftOrder, ParameterOrder.CONSTANT)) {
                return checkAreDistinct(right, _left);
            }
            return visit(right);
        }
        
        protected boolean visit(ConstantCondition right) {
            return defaultDistinctCheck(right);
        }
        
        @Override
        public final Boolean visit(LikeCondition right, ParameterOrder leftOrder) {
            if (swapParameters(leftOrder, ParameterOrder.LIKE)) {
                return checkAreDistinct(right, _left);
            }
            return visit(right);
        }

        protected boolean visit(LikeCondition right) {
            return defaultDistinctCheck(right);
        }

        @Override
        public final Boolean visit(ComparisonCondition right, ParameterOrder leftOrder) {
            if (swapParameters(leftOrder, ParameterOrder.COMPARISON)) {
                return checkAreDistinct(right, _left);
            }
            return visit(right);
        }

        protected boolean visit(ComparisonCondition right) {
            return defaultDistinctCheck(right);
        }

        @Override
        public final Boolean visit(EqualCondition right, ParameterOrder leftOrder) {
            if (swapParameters(leftOrder, ParameterOrder.EQUAL)) {
                return checkAreDistinct(right, _left);
            }
            return visit(right);
        }

        protected boolean visit(EqualCondition right) {
            return defaultDistinctCheck(right);
        }

        @Override
        public final Boolean visit(InCondition right, ParameterOrder leftOrder) {
            if (swapParameters(leftOrder, ParameterOrder.IN)) {
                return checkAreDistinct(right, _left);
            }
            return visit(right);
        }

        protected boolean visit(InCondition right) {
            return defaultDistinctCheck(right);
        }

        @Override
        public final Boolean visit(IntrinsicCondition right, ParameterOrder leftOrder) {
            if (swapParameters(leftOrder, ParameterOrder.INTRINSIC)) {
                return checkAreDistinct(right, _left);
            }
            return visit(right);
        }

        protected boolean visit(IntrinsicCondition right) {
            return defaultDistinctCheck(right);
        }

        @Override
        public final Boolean visit(IsCondition right, ParameterOrder leftOrder) {
            if (swapParameters(leftOrder, ParameterOrder.IS)) {
                return checkAreDistinct(right, _left);
            }
            return visit(right);
        }

        protected boolean visit(IsCondition right) {
            return defaultDistinctCheck(right);
        }

        @Override
        public final Boolean visit(ContainsCondition right, ParameterOrder leftOrder) {
            if (swapParameters(leftOrder, ParameterOrder.CONTAINS)) {
                return checkAreDistinct(right, _left);
            }
            return visit(right);
        }

        protected boolean visit(ContainsCondition right) {
            return defaultDistinctCheck(right);
        }

        @Override
        public final Boolean visit(MapCondition right, ParameterOrder leftOrder) {
            if (swapParameters(leftOrder, ParameterOrder.MAP)) {
                return checkAreDistinct(right, _left);
            }
            return visit(right);
        }

        protected boolean visit(MapCondition right) {
            return defaultDistinctCheck(right);
        }

        /**
         * Future:  Build an actual implementation.  Currently this call is never made and is here only to satisfy
         * the {@link ConditionVisitor} interface.
         */
        @Override
        public final Boolean visit(NotCondition right, ParameterOrder leftOrder) {
            return false;
        }

        /**
         * Future:  Build an actual implementation.  Currently this call is never made and is here only to satisfy
         * the {@link ConditionVisitor} interface.
         */
        @Override
        public final Boolean visit(AndCondition right, ParameterOrder leftOrder) {
            return false;
        }

        /**
         * Future:  Build an actual implementation.  Currently this call is never made and is here only to satisfy
         * the {@link ConditionVisitor} interface.
         */
        @Override
        public final Boolean visit(OrCondition right, ParameterOrder leftOrder) {
            return false;
        }
    }
}
