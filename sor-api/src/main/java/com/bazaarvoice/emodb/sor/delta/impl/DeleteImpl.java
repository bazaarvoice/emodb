package com.bazaarvoice.emodb.sor.delta.impl;

import com.bazaarvoice.emodb.sor.delta.Delete;
import com.bazaarvoice.emodb.sor.delta.DeltaVisitor;

import javax.annotation.Nullable;
import java.io.IOException;

public class DeleteImpl extends AbstractDelta implements Delete {

    public static final Delete INSTANCE = new DeleteImpl();

    private DeleteImpl() {
    }

    @Override
    public <T, V> V visit(DeltaVisitor<T, V> visitor, T context) {
        return visitor.visit(this, context);
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    @Override
    public String toString() {
        return "~";
    }

    @Override
    public void appendTo(Appendable buf) throws IOException {
        buf.append('~');
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        return obj instanceof Delete;
    }

    @Override
    public int hashCode() {
        return 3221;
    }
}
