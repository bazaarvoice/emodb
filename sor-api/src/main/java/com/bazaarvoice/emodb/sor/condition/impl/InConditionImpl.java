package com.bazaarvoice.emodb.sor.condition.impl;

import com.bazaarvoice.emodb.common.json.JsonValidator;
import com.bazaarvoice.emodb.common.json.OrderedJson;
import com.bazaarvoice.emodb.sor.condition.ConditionVisitor;
import com.bazaarvoice.emodb.sor.condition.InCondition;
import com.google.common.base.Joiner;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Set;

public class InConditionImpl extends AbstractCondition implements InCondition {

    private final Set<Object> _values;

    public InConditionImpl(Set<Object> values) {
        for (Object value : values) {
            JsonValidator.checkValid(value);
        }
        _values = values;
    }

    @Override
    public Set<Object> getValues() {
        return _values;
    }

    @Override
    public <T, V> V visit(ConditionVisitor<T, V> visitor, @Nullable T context) {
        return visitor.visit(this, context);
    }

    @Override
    public void appendTo(Appendable buf) throws IOException {
        buf.append("in(");
        Joiner.on(',').appendTo(buf, OrderedJson.orderedStrings(_values));
        buf.append(")");
    }

    @Override
    public boolean equals(Object o) {
        return (this == o) || (o instanceof InCondition) && _values.equals(((InCondition) o).getValues());
    }

    @Override
    public int hashCode() {
        return _values != null ? _values.hashCode() : 0;
    }
}
