package com.bazaarvoice.emodb.sor.delta.impl;

import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.delta.ConditionalDeltaBuilder;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ConditionalDeltaBuilderImpl implements ConditionalDeltaBuilder {

    private final List<Map.Entry<Condition, Delta>> _clauses = Lists.newArrayList();
    private Delta _otherwise;

    @Override
    public ConditionalDeltaBuilder add(Condition condition, Delta delta) {
        _clauses.add(Maps.immutableEntry(requireNonNull(condition, "condition"), requireNonNull(delta, "delta")));
        return this;
    }

    @Override
    public ConditionalDeltaBuilder otherwise(Delta delta) {
        checkArgument(_otherwise == null, "Multiple otherwise deltas.");
        _otherwise = requireNonNull(delta);
        return this;
    }

    @Override
    public Delta build() {
        // Construct a chain of if-then-elif-then-else-end by looping through the clauses in reverse order.
        Delta delta = (_otherwise != null) ? _otherwise : Deltas.noop();
        for (int i = _clauses.size() - 1; i >= 0; i--) {
            Map.Entry<Condition, Delta> clause = _clauses.get(i);
            delta = Deltas.conditional(clause.getKey(), clause.getValue(), delta);
        }
        return delta;
    }
}
