package com.bazaarvoice.emodb.sor.condition.impl;

import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.ConditionVisitor;
import com.bazaarvoice.emodb.sor.condition.OrCondition;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Standard implementation for {@link OrCondition}.  Note that this implementation preserves the original condition
 * order internally for serialization purposes in {@link #appendTo(Appendable)} but the return value from
 * {@link #getConditions()} is sorted by increasing weight.
 */
public class OrConditionImpl extends AbstractCondition implements OrCondition {

    private final Collection<Condition> _conditions;
    private final List<Condition> _weightSortedConditions;

    public OrConditionImpl(Collection<Condition> conditions) {
        _conditions = requireNonNull(conditions, "conditions");
        _weightSortedConditions = Collections.unmodifiableList(
                conditions.stream()
                        .sorted(Comparator.comparingInt(Condition::weight))
                        .collect(Collectors.toList()));

    }

    @Override
    public Collection<Condition> getConditions() {
        return _weightSortedConditions;
    }

    @Override
    public <T, V> V visit(ConditionVisitor<T, V> visitor, @Nullable T context) {
        return visitor.visit(this, context);
    }

    @Override
    public void appendTo(Appendable buf) throws IOException {
        buf.append("or(");
        String sep = "";
        for (Condition condition : _conditions) {
            buf.append(sep);
            condition.appendTo(buf);
            sep = ",";
        }
        buf.append(")");
    }

    /**
     * The worst case total weight of an "or" is the sum of the weights of all contained conditions.
     */
    @Override
    public int weight() {
        return _conditions.stream().mapToInt(Condition::weight).sum();
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof OrCondition) && conditionsEqual(((OrCondition) o).getConditions());
    }

    /**
     * The order of the conditions is irrelevant, just check the set is the same.
     */
    private boolean conditionsEqual(Collection<Condition> conditions) {
        if (conditions.size() != _conditions.size()) {
            return false;
        }
        List<Condition> unvalidatedConditions = new ArrayList<>(conditions);
        for (Condition condition : _conditions) {
            if (!unvalidatedConditions.remove(condition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        // Order of the underlying collection of values is irrelevant, so sum the individual object hashes
        // so that order does not affect the computed hash.
        return 10601 ^ _conditions.stream().mapToInt(Object::hashCode).sum();
    }
}
