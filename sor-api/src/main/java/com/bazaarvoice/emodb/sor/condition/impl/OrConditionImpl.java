package com.bazaarvoice.emodb.sor.condition.impl;

import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.ConditionVisitor;
import com.bazaarvoice.emodb.sor.condition.OrCondition;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;

import static com.google.common.base.Preconditions.checkNotNull;

public class OrConditionImpl extends AbstractCondition implements OrCondition {

    private final Collection<Condition> _conditions;

    public OrConditionImpl(Collection<Condition> conditions) {
        _conditions = checkNotNull(conditions, "conditions");
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
        buf.append("or(");
        String sep = "";
        for (Condition condition : _conditions) {
            buf.append(sep);
            condition.appendTo(buf);
            sep = ",";
        }
        buf.append(")");
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
