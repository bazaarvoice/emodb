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
    public boolean overlaps(ComparisonCondition condition) {
        Comparison otherComparison = condition.getComparison();
        Object otherValue = condition.getValue();

        if (_value instanceof Number ^ otherValue instanceof Number) {
            // The other condition is for a different data type, so they can't overlap
            return false;
        }

        // If both have the same direction then logically they must overlap at some point.
        boolean isGreater = _comparison == Comparison.GT || _comparison == Comparison.GE;
        boolean isOtherGreater = otherComparison == Comparison.GT || otherComparison == Comparison.GE;

        if (isGreater == isOtherGreater) {
            return true;
        }

        //noinspection unchecked
        int compare = isGreater ? ((Comparable) otherValue).compareTo(_value) : ((Comparable) _value).compareTo(otherValue);

        return compare > 0 ||
                (compare == 0 && _comparison.isClosed() && otherComparison.isClosed());
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean isSubsetOf(ComparisonCondition condition) {
        Object lObject = getValue();
        Object rObject = condition.getValue();

        Comparable lValue;
        Comparable rValue;

        if (lObject instanceof Number) {
            if (!(rObject instanceof Number)) {
                return false;
            }
            lValue = ((Number) lObject).doubleValue();
            rValue = ((Number) rObject).doubleValue();
        } else if (lObject instanceof String && rObject instanceof String) {
            lValue = (String) lObject;
            rValue = (String) rObject;
        } else {
            return false;
        }

        switch (getComparison()) {
            case GT:
                switch (condition.getComparison()) {
                    case GT:    // gt(10).isSubsetOf(gt(5))
                    case GE:    // gt(10).isSubsetOf(ge(5))
                        return lValue.compareTo(rValue) >= 0;
                }
                break;

            case GE:
                switch (condition.getComparison()) {
                    case GT:    // ge(10).isSubsetOf(gt(5))
                        return lValue.compareTo(rValue) > 0;
                    case GE:    // ge(10).isSubsetOf(ge(5))
                        return lValue.compareTo(rValue) >= 0;
                }
                break;

            case LT:
                switch (condition.getComparison()) {
                    case LT:    // lt(10).isSubsetOf(lt(15))
                    case LE:    // lt(10).isSubsetOf(le(15))
                        return lValue.compareTo(rValue) <= 0;
                }
                break;

            case LE:
                switch (condition.getComparison()) {
                    case LT:    // le(10).isSubsetOf(lt(15))
                        return lValue.compareTo(rValue) < 0;
                    case LE:    // le(10).isSubsetOf(le(15))
                        return lValue.compareTo(rValue) <= 0;
                }
                break;
        }

        return false;
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
