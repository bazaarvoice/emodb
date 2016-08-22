package com.bazaarvoice.emodb.web.auth.matching;

import com.bazaarvoice.emodb.auth.permissions.matching.ConstantPart;
import com.bazaarvoice.emodb.auth.permissions.matching.Implier;
import com.bazaarvoice.emodb.auth.permissions.matching.MatchingPart;
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
import com.google.common.base.Objects;

import javax.annotation.Nullable;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * ConditionPart can be used to match a permission part using conditionals.  For example:
 *
 * <code>if(not("drop_table"))</code>
 *
 * Since the condition is always used to test Strings only a small subset of Conditions are supported.
 * Conditions that do not have any meaning in this context, such as intrinsic conditions, will raise an
 * IllegalArgumentException.
 */
public class ConditionPart extends EmoMatchingPart {

    private final static ConditionVisitor<String, Void> _validator = new ValidationVisitor();
    private final static ConditionVisitor<String, Boolean> _evaluator = new EvaluationVisitor();

    private final Condition _condition;

    public ConditionPart(Condition condition) {
        _condition = checkNotNull(condition, "condition");
        _condition.visit(_validator, null);
    }

    @Override
    protected boolean impliedBy(Implier implier, List<MatchingPart> leadingParts) {
        return ((EmoImplier) implier).impliesCondition(this, leadingParts);
    }

    @Override
    public boolean isAssignable() {
        return true;
    }

    @Override
    public boolean impliesConstant(ConstantPart part, List<MatchingPart> leadingParts) {
        return _condition.visit(_evaluator, part.getValue());
    }

    @Override
    public boolean impliesCreateTable(CreateTablePart part, List<MatchingPart> leadingParts) {
        // The constant is evaluated in the context of the table name
        return _condition.visit(_evaluator, part.getName());
    }

    @Override
    public boolean impliesAny() {
        // Although there are an infinite number of conditions which satisfy all inputs we only check for the
        // most basic "alwaysTrue()" condition.
        return _condition.equals(Conditions.alwaysTrue());
    }

    /**
     * Base visitor that throws an exception if an unsupported condition is found.
     */
    private static abstract class BaseConditionVisitor<T> implements ConditionVisitor<String, T> {
        @Nullable
        @Override
        public T visit(IntrinsicCondition condition, @Nullable String context) {
            throw new IllegalArgumentException("Value cannot be evaluated as an intrinsic");
        }

        @Nullable
        @Override
        public T visit(IsCondition condition, @Nullable String context) {
            throw new IllegalArgumentException("Value cannot be evaluated as an type");
        }

        @Nullable
        @Override
        public T visit(ComparisonCondition condition, @Nullable String context) {
            throw new IllegalArgumentException("Value cannot be evaluated as a comparable");
        }

        @Nullable
        @Override
        public T visit(ContainsCondition condition, @Nullable String context) {
            throw new IllegalArgumentException("Value cannot be evaluated as an set");
        }

        @Nullable
        @Override
        public T visit(MapCondition condition, @Nullable String context) {
            throw new IllegalArgumentException("Value cannot be evaluated as an map");
        }
    }

    /**
     * Visitor for evaluating whether the condition contains unsupported conditions.
     */
    private final static class ValidationVisitor extends BaseConditionVisitor<Void> {
        @Nullable
        @Override
        public Void visit(ConstantCondition condition, @Nullable String context) {
            return null;
        }

        @Nullable
        @Override
        public Void visit(EqualCondition condition, @Nullable String context) {
            return null;
        }

        @Nullable
        @Override
        public Void visit(InCondition condition, @Nullable String context) {
            return null;
        }

        @Nullable
        @Override
        public Void visit(NotCondition condition, @Nullable String context) {
            return condition.getCondition().visit(this, context);
        }

        @Nullable
        @Override
        public Void visit(AndCondition condition, @Nullable String context) {
            for (Condition c : condition.getConditions()) {
                c.visit(this, null);
            }
            return null;
        }

        @Nullable
        @Override
        public Void visit(OrCondition condition, @Nullable String context) {
            for (Condition c : condition.getConditions()) {
                c.visit(this, null);
            }
            return null;
        }

        @Nullable
        @Override
        public Void visit(LikeCondition condition, @Nullable String context) {
            return null;
        }
    }

    /**
     * Visitor for evaluating whether an input string matches the condition.
     */
    private final static class EvaluationVisitor extends BaseConditionVisitor<Boolean> {
        @Nullable
        @Override
        public Boolean visit(ConstantCondition condition, @Nullable String context) {
            return condition.getValue();
        }

        @Nullable
        @Override
        public Boolean visit(EqualCondition condition, @Nullable String context) {
            return Objects.equal(condition.getValue(), context);
        }

        @Nullable
        @Override
        public Boolean visit(InCondition condition, @Nullable String context) {
            return condition.getValues().contains(context);
        }

        @Nullable
        @Override
        public Boolean visit(NotCondition condition, @Nullable String context) {
            return !condition.getCondition().visit(this, context);
        }

        @Nullable
        @Override
        public Boolean visit(AndCondition condition, @Nullable String context) {
            for (Condition c : condition.getConditions()) {
                if (!c.visit(this, context)) {
                    return false;
                }
            }
            return true;
        }

        @Nullable
        @Override
        public Boolean visit(OrCondition condition, @Nullable String context) {
            for (Condition c : condition.getConditions()) {
                if (c.visit(this, context)) {
                    return true;
                }
            }
            return false;
        }

        @Nullable
        @Override
        public Boolean visit(LikeCondition condition, @Nullable String context) {
            return context != null && condition.matches(context);
        }
    }
}
