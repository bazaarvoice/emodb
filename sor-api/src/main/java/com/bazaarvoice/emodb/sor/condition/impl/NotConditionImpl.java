package com.bazaarvoice.emodb.sor.condition.impl;

import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.ConditionVisitor;
import com.bazaarvoice.emodb.sor.condition.NotCondition;

import javax.annotation.Nullable;
import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class NotConditionImpl extends AbstractCondition implements NotCondition {

    private final Condition _condition;

    public NotConditionImpl(Condition condition) {
        _condition = requireNonNull(condition, "condition");
    }

    @Override
    public Condition getCondition() {
        return _condition;
    }

    @Override
    public <T, V> V visit(ConditionVisitor<T, V> visitor, @Nullable T context) {
        return visitor.visit(this, context);
    }

    @Override
    public void appendTo(Appendable buf) throws IOException {
        buf.append("not(");
        _condition.appendTo(buf);
        buf.append(")");
    }

    @Override
    public boolean equals(Object o) {
        return (this == o) || (o instanceof NotCondition) && _condition.equals(((NotCondition) o).getCondition());
    }

    @Override
    public int hashCode() {
        return 72019 ^ _condition.hashCode();
    }
}
