package com.bazaarvoice.emodb.sor.delta.impl;

import com.bazaarvoice.emodb.sor.delta.DeltaVisitor;
import com.bazaarvoice.emodb.sor.delta.NoopDelta;

import javax.annotation.Nullable;
import java.io.IOException;

public class NoopDeltaImpl extends AbstractDelta implements NoopDelta {

    public static final NoopDelta INSTANCE = new NoopDeltaImpl();

    private NoopDeltaImpl() {
    }

    @Override
    public <T, V> V visit(DeltaVisitor<T, V> visitor, T context) {
        return visitor.visit(this, context);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public String toString() {
        return "..";
    }

    @Override
    public void appendTo(Appendable buf) throws IOException {
        buf.append("..");
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        return obj instanceof NoopDelta;
    }

    @Override
    public int hashCode() {
        return 1259;
    }
}
