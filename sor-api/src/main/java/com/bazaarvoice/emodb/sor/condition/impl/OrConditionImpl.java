package com.bazaarvoice.emodb.sor.condition.impl;

import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.ConditionVisitor;
import com.bazaarvoice.emodb.sor.condition.OrCondition;
import com.bazaarvoice.emodb.streaming.AppendableJoiner;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;

import static java.util.Objects.requireNonNull;

public class OrConditionImpl extends AbstractCondition implements OrCondition {

    private final Collection<Condition> _conditions;

    public OrConditionImpl(Collection<Condition> conditions) {
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
        _conditions.stream().collect(AppendableJoiner.joining(buf, ",", "or(", ")", (app, cond) -> cond.appendTo(app)));
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof OrCondition) && _conditions.equals(((OrCondition) o).getConditions());
    }

    @Override
    public int hashCode() {
        return 10601 ^ _conditions.hashCode();
    }
}
