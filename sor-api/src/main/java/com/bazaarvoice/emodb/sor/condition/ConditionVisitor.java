package com.bazaarvoice.emodb.sor.condition;

import javax.annotation.Nullable;

public interface ConditionVisitor<T, V> {

    @Nullable
    V visit(ConstantCondition condition, @Nullable T context);

    @Nullable
    V visit(EqualCondition condition, @Nullable T context);

    @Nullable
    V visit(InCondition condition, @Nullable T context);

    @Nullable
    V visit(IntrinsicCondition condition, @Nullable T context);

    @Nullable
    V visit(IsCondition condition, @Nullable T context);

    @Nullable
    V visit(ComparisonCondition condition, @Nullable T context);

    @Nullable
    V visit(ContainsCondition condition, @Nullable T context);

    @Nullable
    V visit(LikeCondition condition, @Nullable T context);

    @Nullable
    V visit(NotCondition condition, @Nullable T context);

    @Nullable
    V visit(AndCondition condition, @Nullable T context);

    @Nullable
    V visit(OrCondition condition, @Nullable T context);

    @Nullable
    V visit(MapCondition condition, @Nullable T context);
}
