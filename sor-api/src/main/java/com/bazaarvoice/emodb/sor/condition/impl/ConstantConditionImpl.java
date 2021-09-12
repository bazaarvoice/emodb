package com.bazaarvoice.emodb.sor.condition.impl;

import com.bazaarvoice.emodb.sor.condition.ConditionVisitor;
import com.bazaarvoice.emodb.sor.condition.ConstantCondition;

import javax.annotation.Nullable;
import java.io.IOException;

public class ConstantConditionImpl extends AbstractCondition implements ConstantCondition {

    public static final ConstantConditionImpl TRUE = new ConstantConditionImpl(true);
    public static final ConstantConditionImpl FALSE = new ConstantConditionImpl(false);

    private final boolean _value;

    private ConstantConditionImpl(boolean value) {
        _value = value;
    }

    @Override
    public boolean getValue() {
        return _value;
    }

    @Override
    public <T, V> V visit(ConditionVisitor<T, V> visitor, @Nullable T context) {
        return visitor.visit(this, context);
    }

    @Override
    public void appendTo(Appendable buf) throws IOException {
        buf.append(_value ? "alwaysTrue()" : "alwaysFalse()");
    }

    /**
     * Constant conditions are effectively free since the result is simply "true" or "false" with no computation necessary.
     */
    @Override
    public int weight() {
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof ConstantCondition) && _value == ((ConstantCondition) o).getValue();
    }

    @Override
    public int hashCode() {
        return _value ? 15919 : 773;
    }
}
