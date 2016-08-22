package com.bazaarvoice.emodb.sor.delta.impl;

import com.bazaarvoice.emodb.common.json.OrderedJson;
import com.bazaarvoice.emodb.sor.condition.ConditionVisitor;
import com.bazaarvoice.emodb.sor.condition.ContainsCondition;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContainsConditionImpl implements ContainsCondition {

    private final Set<Object> _values;
    private final Containment _containment;

    public ContainsConditionImpl(Object value) {
        _values = Sets.newLinkedHashSet();
        _values.add(value);
        _containment = Containment.ALL;
    }

    public ContainsConditionImpl(Set<Object> values, Containment containment) {
        _values = checkNotNull(values, "values");
        // If there is only a single value with "contains any" then "contains all" is implied
        if (values.size() == 1 &&  containment == Containment.ANY) {
            _containment = Containment.ALL;
        } else {
            _containment = containment;
        }
    }

    @Override
    public Set<Object> getValues() {
        return _values;
    }

    @Override
    public Containment getContainment() {
        return _containment;
    }

    @Override
    public <T, V> V visit(ConditionVisitor<T, V> visitor, @Nullable T context) {
        return visitor.visit(this, context);
    }

    @Override
    public void appendTo(Appendable buf) throws IOException {
        if (_values.size() == 1 && _containment != Containment.ONLY) {
            buf.append("contains(");
        } else {
            buf.append("contains").append(_containment.getSuffix()).append("(");
        }
        Joiner.on(',').appendTo(buf, OrderedJson.orderedStrings(_values));
        buf.append(")");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ContainsConditionImpl)) {
            return false;
        }

        ContainsConditionImpl that = (ContainsConditionImpl) o;

        return _containment == that._containment &&
                _values.equals(that._values);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_values, _containment);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        try {
            appendTo(builder);
        } catch (IOException e) {
            // Should never happen
            throw Throwables.propagate(e);
        }
        return builder.toString();
    }
}
