package com.bazaarvoice.emodb.sor.condition.impl;

import com.bazaarvoice.emodb.sor.condition.Condition;
import com.google.common.base.Throwables;

import java.io.IOException;

public abstract class AbstractCondition implements Condition {

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
}
