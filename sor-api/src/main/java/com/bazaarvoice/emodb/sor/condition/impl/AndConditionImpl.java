package com.bazaarvoice.emodb.sor.condition.impl;

import com.bazaarvoice.emodb.sor.condition.AndCondition;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.ConditionVisitor;
import com.bazaarvoice.emodb.streaming.AppendableJoiner;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;

import static java.util.Objects.requireNonNull;

public class AndConditionImpl extends AbstractCondition implements AndCondition {

    private final Collection<Condition> _conditions;

    public AndConditionImpl(Collection<Condition> conditions) {
        _conditions = requireNonNull(conditions, "conditions");
    }

    @Override
    public Collection<Condition> getConditions() {
        return _conditions;
    }

    @Override
    public <T, V> V visit(ConditionVisitor<T, V> visitor, @Nullable T context) {
        return visitor.visit(this, context);
    }

    @Override
    public void appendTo(Appendable buf) throws IOException {
        _conditions.stream().collect(AppendableJoiner.joining(buf, ",", "and(", ")", (app, cond) -> cond.appendTo(app)));
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof AndCondition) && _conditions.equals(((AndCondition) o).getConditions());
    }

    @Override
    public int hashCode() {
        return 11213 ^ _conditions.hashCode();
    }
}
