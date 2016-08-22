package com.bazaarvoice.emodb.sor.condition.impl;

import com.bazaarvoice.emodb.sor.condition.AndCondition;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.ConditionVisitor;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;

import static com.google.common.base.Preconditions.checkNotNull;

public class AndConditionImpl extends AbstractCondition implements AndCondition {

    private final Collection<Condition> _conditions;

    public AndConditionImpl(Collection<Condition> conditions) {
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
        buf.append("and(");
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
        return this == o || (o instanceof AndCondition) && _conditions.equals(((AndCondition) o).getConditions());
    }

    @Override
    public int hashCode() {
        return 11213 ^ _conditions.hashCode();
    }
}
