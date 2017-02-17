package com.bazaarvoice.emodb.web.privacy;

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
import com.bazaarvoice.emodb.sor.condition.impl.AndConditionImpl;
import com.bazaarvoice.emodb.sor.condition.impl.ComparisonConditionImpl;
import com.bazaarvoice.emodb.sor.condition.impl.EqualConditionImpl;
import com.bazaarvoice.emodb.sor.condition.impl.InConditionImpl;
import com.bazaarvoice.emodb.sor.condition.impl.IntrinsicConditionImpl;
import com.bazaarvoice.emodb.sor.condition.impl.MapConditionImpl;
import com.bazaarvoice.emodb.sor.condition.impl.NotConditionImpl;
import com.bazaarvoice.emodb.sor.condition.impl.OrConditionImpl;
import com.bazaarvoice.emodb.sor.delta.impl.ContainsConditionImpl;

import javax.annotation.Nullable;

import static com.bazaarvoice.emodb.web.privacy.HiddenFieldStripper.stripHiddenDispatch;

class StrippingConditionVisitor implements ConditionVisitor<Void, Condition> {
    @Nullable @Override public Condition visit(final ConstantCondition condition, @Nullable final Void context) {
        return condition;
    }

    @Nullable @Override public Condition visit(final EqualCondition condition, @Nullable final Void context) {
        return new EqualConditionImpl(stripHiddenDispatch(condition.getValue()));
    }

    @Nullable @Override public Condition visit(final InCondition condition, @Nullable final Void context) {
        return new InConditionImpl(stripHiddenDispatch(condition.getValues()));
    }

    @Nullable @Override public Condition visit(final IntrinsicCondition condition, @Nullable final Void context) {
        return new IntrinsicConditionImpl(condition.getName(), condition.visit(this, null));
    }

    @Nullable @Override public Condition visit(final IsCondition condition, @Nullable final Void context) {
        return condition;
    }

    @Nullable @Override public Condition visit(final ComparisonCondition condition, @Nullable final Void context) {
        return new ComparisonConditionImpl(condition.getComparison(), stripHiddenDispatch(condition.getValue()));
    }

    @Nullable @Override public Condition visit(final ContainsCondition condition, @Nullable final Void context) {
        return new ContainsConditionImpl(stripHiddenDispatch(condition.getValues()), condition.getContainment());
    }

    @Nullable @Override public Condition visit(final LikeCondition condition, @Nullable final Void context) {
        return condition;
    }

    @Nullable @Override public Condition visit(final NotCondition condition, @Nullable final Void context) {
        return new NotConditionImpl(condition.getCondition().visit(this, null));
    }

    @Nullable @Override public Condition visit(final AndCondition condition, @Nullable final Void context) {
        return new AndConditionImpl(stripHiddenDispatch(condition.getConditions()));
    }

    @Nullable @Override public Condition visit(final OrCondition condition, @Nullable final Void context) {
        return new OrConditionImpl(stripHiddenDispatch(condition.getConditions()));
    }

    @Nullable @Override public Condition visit(final MapCondition condition, @Nullable final Void context) {
        return new MapConditionImpl(stripHiddenDispatch(condition.getEntries()));
    }
}
