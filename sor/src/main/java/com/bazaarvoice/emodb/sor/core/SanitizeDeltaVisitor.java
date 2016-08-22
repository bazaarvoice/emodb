package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.delta.ConditionalDelta;
import com.bazaarvoice.emodb.sor.delta.Delete;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.DeltaVisitor;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.delta.Literal;
import com.bazaarvoice.emodb.sor.delta.MapDelta;
import com.bazaarvoice.emodb.sor.delta.NoopDelta;
import com.bazaarvoice.emodb.sor.delta.SetDelta;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Sanitizes a {@link Delta} destined for the System of Record.
 * Throws an exception if the delta could insert a
 * top-level JSON value that's not a JSON object (ie. a {@code Map}) and strips out top-level intrinsic keys (including ~tags).
 */
class SanitizeDeltaVisitor implements DeltaVisitor<Void, Delta> {

    private static final SanitizeDeltaVisitor _instance = new SanitizeDeltaVisitor();

    // We don't want to add TAGS as an intrinsic field
    private static final Set<String> _excludedKeys = Sets.union(Intrinsic.DATA_FIELDS,
            ImmutableSet.of(UpdateRef.TAGS_NAME));

    private SanitizeDeltaVisitor() {
    }

    static Delta sanitize(Delta delta) {
        return delta.visit(_instance, null);
    }

    @Override
    public Delta visit(NoopDelta delta, Void ignore) {
        return delta;
    }

    @Override
    public Delta visit(Delete delta, Void ignore) {
        return delta;
    }

    @Override
    public Delta visit(Literal delta, Void ignore) {
        Object json = delta.getValue();
        checkArgument(json instanceof Map, "Top-level values in the System of Record must be JSON objects.");
        return Deltas.literal(excludeKeys((Map<?, ?>) json, _excludedKeys));
    }

    @Override
    public Delta visit(MapDelta delta, Void ignore) {
        return Deltas.mapBuilder()
                .removeRest(delta.getRemoveRest())
                .deleteIfEmpty(delta.getDeleteIfEmpty())
                .updateAll(excludeKeys(delta.getEntries(), _excludedKeys))
                .build();
    }

    @Nullable
    @Override
    public Delta visit(SetDelta delta, @Nullable Void context) {
        throw new IllegalArgumentException("Top-level values in the System of Record must be JSON objects.");
    }

    @Override
    public Delta visit(ConditionalDelta delta, Void ignore) {
        return Deltas.conditional(delta.getTest(),
                delta.getThen().visit(this, null),
                delta.getElse().visit(this, null));
    }

    private <K, V> Map<K, V> excludeKeys(Map<K, V> map, Set<String> keys) {
        return Maps.filterKeys(map, Predicates.not(Predicates.<Object>in(keys)));
    }
}
