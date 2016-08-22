package com.bazaarvoice.emodb.sor.delta.impl;

import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.delta.ConditionalDelta;
import com.bazaarvoice.emodb.sor.delta.Delete;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.DeltaVisitor;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.delta.Literal;
import com.bazaarvoice.emodb.sor.delta.MapDelta;
import com.bazaarvoice.emodb.sor.delta.MapDeltaBuilder;
import com.bazaarvoice.emodb.sor.delta.NoopDelta;
import com.bazaarvoice.emodb.sor.delta.SetDelta;
import com.bazaarvoice.emodb.sor.delta.eval.DeltaEvaluator;
import com.bazaarvoice.emodb.sor.delta.eval.Intrinsics;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class MapDeltaBuilderImpl implements MapDeltaBuilder {

    private boolean _removeRest;
    private final Map<String, Delta> _entries = Maps.newHashMap();
    private boolean _deleteIfEmpty;

    @Override
    public MapDeltaBuilder remove(String key) {
        return update(key, Deltas.delete());
    }

    @Override
    public MapDeltaBuilder removeAll(String... key) {
        return removeAll(Arrays.asList(key));
    }

    @Override
    public MapDeltaBuilder removeAll(Iterable<String> keys) {
        for (String key : keys) {
            remove(key);
        }
        return this;
    }

    @Override
    public MapDeltaBuilder remove(String key, @Nullable Object json) {
        return update(key, Deltas.conditional(Conditions.equal(json), Deltas.delete()));
    }

    @Override
    public MapDeltaBuilder removeAll(Map<String, ?> json) {
        for (Map.Entry<String, ?> entry : json.entrySet()) {
            remove(entry.getKey(), entry.getValue());
        }
        return this;
    }

    @Override
    public MapDeltaBuilder put(String key, @Nullable Object json) {
        return update(key, Deltas.literal(json));
    }

    @Override
    public MapDeltaBuilder putAll(Map<String, ?> json) {
        for (Map.Entry<String, ?> entry : json.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
        return this;
    }

    @Override
    public MapDeltaBuilder putIfAbsent(String key, @Nullable Object json) {
        return update(key, Deltas.conditional(Conditions.isUndefined(), Deltas.literal(json)));
    }

    @Override
    public MapDeltaBuilder update(String key, Delta delta) {
        checkArgument(!_entries.containsKey(key), "Multiple operations against the same key are not allowed: %s", key);
        _entries.put(key, delta);
        return this;
    }

    @Override
    public MapDeltaBuilder updateAll(Map<String, Delta> deltas) {
        for (Map.Entry<String, Delta> entry : deltas.entrySet()) {
            update(entry.getKey(), entry.getValue());
        }
        return this;
    }

    @Override
    public MapDeltaBuilder updateIfExists(String key, Delta delta) {
        return update(key, Deltas.conditional(Conditions.isDefined(), delta));
    }

    @Override
    public MapDeltaBuilder retain(String key) {
        return update(key, Deltas.noop());
    }

    @Override
    public MapDeltaBuilder retainAll(String... keys) {
        return retainAll(Arrays.asList(keys));
    }

    @Override
    public MapDeltaBuilder retainAll(Iterable<String> keys) {
        for (String key : keys) {
            retain(key);
        }
        return this;
    }

    @Override
    public MapDeltaBuilder deleteIfEmpty() {
        return deleteIfEmpty(true);
    }

    @Override
    public MapDeltaBuilder deleteIfEmpty(boolean deleteIfEmpty) {
        _deleteIfEmpty = deleteIfEmpty;
        return this;
    }

    @Override
    public MapDeltaBuilder removeRest() {
        return removeRest(true);
    }

    @Override
    public MapDeltaBuilder removeRest(boolean removeRest) {
        _removeRest = removeRest;
        return this;
    }

    @Override
    public Delta build() {
        // an add can no-op only if it evaluates to a delete operation.  if we're guaranteed that
        // at least one add doesn't delete, no point in deleting if empty.
        if (_deleteIfEmpty && !_entries.isEmpty() && Iterables.any(_entries.values(), NeverDeletePredicate.INSTANCE)) {
            _deleteIfEmpty = false;
        }
        Delta delta = new MapDeltaImpl(_removeRest, _entries, _deleteIfEmpty);
        if (delta.isConstant()) {
            delta = evalAsConstant(delta);
        }
        return delta;
    }

    private Delta evalAsConstant(Delta delta) {
        Intrinsics intrinsics = null;  // Intrinsics are only referred to by non-constant deltas, so ok to pass null.
        Object result = DeltaEvaluator.eval(delta, DeltaEvaluator.UNDEFINED, intrinsics);
        return (result == DeltaEvaluator.UNDEFINED) ? Deltas.delete() : Deltas.literal(result);
    }

    /**
     * Returns true if a {@link Delta} will never evaluate to {@link DeltaEvaluator#UNDEFINED}.
     */
    private static class NeverDeletePredicate implements Predicate<Delta>, DeltaVisitor<Void, Boolean> {
        private static final NeverDeletePredicate INSTANCE = new NeverDeletePredicate();

        @Override
        public boolean apply(Delta delta) {
            return delta.visit(this, null);
        }

        @Override
        public Boolean visit(Literal delta, @Nullable Void context) {
            return true;
        }

        @Override
        public Boolean visit(NoopDelta delta, @Nullable Void context) {
            return false;
        }

        @Override
        public Boolean visit(Delete delta, @Nullable Void context) {
            return false;
        }

        @Override
        public Boolean visit(MapDelta delta, @Nullable Void context) {
            return !delta.getDeleteIfEmpty();
        }

        @Override
        public Boolean visit(ConditionalDelta delta, @Nullable Void context) {
            return delta.getThen().visit(this, null) && delta.getElse().visit(this, null);
        }

        @Override
        public Boolean visit(SetDelta delta, @Nullable Void context) {
            return !delta.getDeleteIfEmpty();
        }
    }
}
