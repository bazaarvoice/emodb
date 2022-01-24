package com.bazaarvoice.emodb.sor.delta.impl;

import com.bazaarvoice.emodb.sor.delta.Delta;
import com.google.common.base.Throwables;

import java.io.IOException;

public abstract class AbstractDelta implements Delta {

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        try {
            appendTo(buf);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return buf.toString();
    }

    @Override
    public int size() {
        return toString().getBytes().length;
    }
}
