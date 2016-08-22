package com.bazaarvoice.emodb.sor.delta.impl;

import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.delta.ConditionalDelta;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.DeltaVisitor;
import com.bazaarvoice.emodb.sor.delta.NoopDelta;

import javax.annotation.Nullable;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class ConditionalDeltaImpl extends AbstractDelta implements ConditionalDelta {

    private final Condition _test;
    private final Delta _then;
    private final Delta _else;

    public ConditionalDeltaImpl(Condition test, Delta then, Delta anElse) {
        _test = checkNotNull(test);
        _then = checkNotNull(then);
        _else = checkNotNull(anElse);
    }

    @Override
    public Condition getTest() {
        return _test;
    }

    @Override
    public Delta getThen() {
        return _then;
    }

    @Override
    public Delta getElse() {
        return _else;
    }

    @Override
    public <T, V> V visit(DeltaVisitor<T, V> visitor, @Nullable T context) {
        return visitor.visit(this, context);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public void appendTo(Appendable buf) throws IOException {
        buf.append("if ");
        _test.appendTo(buf);
        buf.append(" then ");
        _then.appendTo(buf);
        if (_else instanceof ConditionalDelta) {
            // append the first half of "elif".  the else clause will append the second half and the end.
            buf.append(" el");
            _else.appendTo(buf);
            return;
        }
        if (!(_else instanceof NoopDelta)) {
            buf.append(" else ");
            _else.appendTo(buf);
        }
        buf.append(" end");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConditionalDelta)) {
            return false;
        }
        ConditionalDelta that = (ConditionalDelta) o;
        return _test.equals(that.getTest()) &&
                _then.equals(that.getThen()) &&
                _else.equals(that.getElse());
    }

    @Override
    public int hashCode() {
        return 31 * (31 * _test.hashCode() + _then.hashCode()) + _else.hashCode();
    }
}
