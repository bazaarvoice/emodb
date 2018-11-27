package com.bazaarvoice.emodb.sor.condition.impl;

import com.bazaarvoice.emodb.common.json.JsonValidator;
import com.bazaarvoice.emodb.common.json.OrderedJson;
import com.bazaarvoice.emodb.sor.condition.ConditionVisitor;
import com.bazaarvoice.emodb.sor.condition.EqualCondition;
import com.bazaarvoice.emodb.sor.delta.deser.DeltaJson;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;

public class EqualConditionImpl extends AbstractCondition implements EqualCondition {

    private final Object _value;

    public EqualConditionImpl(@Nullable Object value) {
        _value = JsonValidator.checkValid(value);
    }

    @Override
    public Object getValue() {
        return _value;
    }

    @Override
    public <T, V> V visit(ConditionVisitor<T, V> visitor, @Nullable T context) {
        return visitor.visit(this, context);
    }

    @Override
    public void appendTo(Appendable buf) throws IOException {
        DeltaJson.append(buf, OrderedJson.ordered(_value));
    }

    @Override
    public boolean equals(Object o) {
        return (this == o) || (o instanceof EqualCondition) && Objects.equals(_value, ((EqualCondition) o).getValue());
    }

    @Override
    public int hashCode() {
        return _value != null ? _value.hashCode() : 0;
    }
}
