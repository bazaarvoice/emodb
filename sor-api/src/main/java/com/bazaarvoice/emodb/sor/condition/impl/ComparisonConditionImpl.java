package com.bazaarvoice.emodb.sor.condition.impl;

import com.bazaarvoice.emodb.sor.condition.Comparison;
import com.bazaarvoice.emodb.sor.condition.ComparisonCondition;
import com.bazaarvoice.emodb.sor.condition.ConditionVisitor;
import com.bazaarvoice.emodb.sor.delta.deser.DeltaJson;
import com.google.common.base.Objects;
import com.google.common.io.CharStreams;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Writer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ComparisonConditionImpl extends AbstractCondition implements ComparisonCondition {

    private final Comparison _comparison;
    private final Object _value;

    public ComparisonConditionImpl(Comparison comparison, Object value) {
        _comparison = checkNotNull(comparison, "comparison");
        _value = checkNotNull(value, "value");
        checkArgument(value instanceof Number || value instanceof String, "%s only supports numbers and strings", comparison.getDeltaFunction());
    }

    @Override
    public Comparison getComparison() {
        return _comparison;
    }

    @Override
    public Object getValue() {
        return _value;
    }

    @Override
    public void appendTo(Appendable buf)
            throws IOException {
        // Use a writer so the value can be correctly converted to json using DeltaJson.
        Writer out = CharStreams.asWriter(buf);
        out.write(_comparison.getDeltaFunction());
        out.write("(");
        DeltaJson.write(out, _value);
        out.write(")");
    }

    @Override
    public <T, V> V visit(ConditionVisitor<T, V> visitor, @Nullable T context) {
        return visitor.visit(this, context);
    }

    @Override
    public boolean equals(Object o) {
        return (this == o) || (
                o instanceof ComparisonCondition &&
                _comparison == ((ComparisonCondition)o).getComparison() &&
                _value.equals(((ComparisonCondition)o).getValue()));
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_comparison, _value);
    }
}
