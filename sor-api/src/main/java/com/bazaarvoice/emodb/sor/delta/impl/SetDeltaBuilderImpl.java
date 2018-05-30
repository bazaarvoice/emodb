package com.bazaarvoice.emodb.sor.delta.impl;

import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.delta.Literal;
import com.bazaarvoice.emodb.sor.delta.SetDeltaBuilder;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class SetDeltaBuilderImpl implements SetDeltaBuilder {

    private boolean _removeRest;
    private final Set<Literal> _addedValues = new HashSet<>();
    private final Set<Literal> _removedValues = new HashSet<>();
    private boolean _deleteIfEmpty;

    @Override
    public SetDeltaBuilder remove(@Nullable Object value) {
        Literal literal = value instanceof Literal ? (Literal) value : Deltas.literal(value);
        if (_addedValues.contains(literal) || !_removedValues.add(literal)) {
            throw new IllegalArgumentException(String.format(
                    "Multiple operations against the same value are not allowed: %s", value));
        }
        return this;
    }

    @Override
    public SetDeltaBuilder removeAll(Object... values) {
        return removeAll(Arrays.asList(values));
    }

    @Override
    public SetDeltaBuilder removeAll(Iterable<Object> values) {
        for (Object value : values) {
            remove(value);
        }
        return this;
    }

    @Override
    public SetDeltaBuilder add(@Nullable Object value) {
        Literal literal = value instanceof Literal ? (Literal) value : Deltas.literal(value);
        if (_removedValues.contains(literal) || !_addedValues.add(literal)) {
            throw new IllegalArgumentException(String.format(
                    "Multiple operations against the same value are not allowed: %s", value));
        }
        return this;
    }

    @Override
    public SetDeltaBuilder addAll(Object... values) {
        return addAll(Arrays.asList(values));
    }

    @Override
    public SetDeltaBuilder addAll(Iterable<Object> values) {
        for (Object value : values) {
            add(value);
        }
        return this;
    }

    @Override
    public SetDeltaBuilder deleteIfEmpty() {
        return deleteIfEmpty(true);
    }

    @Override
    public SetDeltaBuilder deleteIfEmpty(boolean deleteIfEmpty) {
        _deleteIfEmpty = deleteIfEmpty;
        return this;
    }

    @Override
    public SetDeltaBuilder removeRest() {
        return removeRest(true);
    }

    @Override
    public SetDeltaBuilder removeRest(boolean removeRest) {
        _removeRest = removeRest;
        return this;
    }

    @Override
    public Delta build() {
        return new SetDeltaImpl(_removeRest, _addedValues, _removedValues, _deleteIfEmpty);
    }
}
