package com.bazaarvoice.emodb.sor.condition.impl;

import com.bazaarvoice.emodb.sor.condition.ConditionVisitor;
import com.bazaarvoice.emodb.sor.condition.IsCondition;
import com.bazaarvoice.emodb.sor.condition.State;

import javax.annotation.Nullable;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class IsConditionImpl extends AbstractCondition implements IsCondition {

    private final State _state;

    public IsConditionImpl(State state) {
        _state = checkNotNull(state);
    }

    @Override
    public State getState() {
        return _state;
    }

    @Override
    public <T, V> V visit(ConditionVisitor<T, V> visitor, @Nullable T context) {
        return visitor.visit(this, context);
    }

    @Override
    public void appendTo(Appendable buf) throws IOException {
        // There is special formatting for particularly common states.
        if (_state == State.UNDEFINED) {
            buf.append('~');
        } else if (_state == State.DEFINED) {
            buf.append('+');
        } else {
            buf.append("is(");
            buf.append(_state.name().toLowerCase());
            buf.append(")");
        }
    }

    @Override
    public boolean equals(Object o) {
        return (this == o) || (o instanceof IsCondition) && _state == ((IsCondition) o).getState();
    }

    @Override
    public int hashCode() {
        return 32411 ^ _state.hashCode();
    }
}
