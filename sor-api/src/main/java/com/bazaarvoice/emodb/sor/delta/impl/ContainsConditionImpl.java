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

import static java.util.Objects.requireNonNull;

public class ContainsConditionImpl implements ContainsCondition {

    private final Set<Object> _values;
    private final Containment _containment;

    public ContainsConditionImpl(Object value) {
        _values = Sets.newLinkedHashSet();
        _values.add(value);
        _containment = Containment.ALL;
    }

    public ContainsConditionImpl(Set<Object> values, Containment containment) {
        _values = requireNonNull(values, "values");
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
    public boolean isSubsetOf(ContainsCondition condition) {
        requireNonNull(condition, "condition");

        Set<Object> lValues = getValues();
        Set<Object> rValues = condition.getValues();

        switch (getContainment()) {
            case ONLY:
                switch (condition.getContainment()) {
                    case ONLY:  // containsOnly("car", "truck").isSubsetOf(containsOnly("car", "truck"))
                        return lValues.equals(rValues);
                    case ANY:   // containsOnly("car", "truck").isSubsetOf(containsAny("car", "truck", "boat"))
                        return !Sets.intersection(lValues, rValues).isEmpty();
                    case ALL:   // containsOnly("car", "truck").isSubsetOf(containsAll("car", "truck", "boat"))
                        return rValues.containsAll(lValues);
                }
                break;

            case ANY:
                switch (condition.getContainment()) {
                    case ANY:   // containsAny("car", "truck").isSubsetOf(containsAny("car", "truck", "boat"))
                        return rValues.containsAll(lValues);
                    case ALL:   // containsAny("car").isSubsetOf(containsAll("car"))
                        return lValues.size() == 1 && lValues.equals(rValues);
                }
                break;

            case ALL:
                switch (condition.getContainment()) {
                    case ANY:   // containsAll("car", "truck").isSubsetOf(containsAny("car", "truck", "boat"))
                        return !Sets.intersection(lValues, rValues).isEmpty();
                    case ALL:   // containsAll("car", "truck", "boat").isSubsetOf(containsAll("car", "truck"))
                        return lValues.containsAll(rValues);
                }
                break;
        }

        return false;
    }

    @Override
    public boolean overlaps(ContainsCondition condition) {
        // By definition containsAll and containsAny do not restrict any additional values besides those specified.
        // Therefore the only conditions that don't overlap are two containsOnly() conditions where the values
        // do not match.
        return getContainment() != Containment.ONLY || condition.getContainment() != Containment.ONLY ||
                getValues().equals(condition.getValues());
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

    /**
     * "Contains" more-so than other conditions is difficult to determine the weight for a-priori since its evaluation
     * time in the worst case scales linearly based on the input size.  For this reason the weight is skewed higher
     * than normal.
     */
    @Override
    public int weight() {
        return 3 * _values.size();
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
            Throwables.propagateIfPossible(e);
            throw new RuntimeException(e);
        }
        return builder.toString();
    }
}
