package com.bazaarvoice.emodb.sor.delta.eval;

import com.bazaarvoice.emodb.sor.condition.eval.ConditionEvaluator;
import com.bazaarvoice.emodb.sor.delta.ConditionalDelta;
import com.bazaarvoice.emodb.sor.delta.Delete;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.DeltaVisitor;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.delta.Literal;
import com.bazaarvoice.emodb.sor.delta.MapDelta;
import com.bazaarvoice.emodb.sor.delta.NoopDelta;
import com.bazaarvoice.emodb.sor.delta.SetDelta;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Applies a sequence of {@link Delta} operations to JSON object.
 */
public class DeltaEvaluator implements DeltaVisitor<Object, Object> {

    public static final Object UNDEFINED = new Object() {
        @Override
        public String toString() {
            return "<UNDEFINED>";
        }
    };

    private final Intrinsics _intrinsics;

    public DeltaEvaluator(Intrinsics intrinsics) {
        _intrinsics = intrinsics;
    }

    @Nullable
    public static Object eval(Delta delta, @Nullable Object json, Intrinsics intrinsics) {
        return delta.visit(new DeltaEvaluator(intrinsics), json);
    }

    @Override
    public Object visit(Delete delta, @Nullable Object json) {
        return UNDEFINED;
    }

    @Override
    @Nullable
    public Object visit(Literal delta, @Nullable Object json) {
        return delta.getValue();
    }

    @Override
    public Object visit(NoopDelta delta, @Nullable Object json) {
        return json;
    }

    @Override
    public Object visit(MapDelta delta, @Nullable Object json) {
        Map<Object, Object> result;
        if (json instanceof Map) {
            //noinspection unchecked
            Map<String, ?> map = (Map<String, ?>) json;
            result = new HashMap<>(map.size() + delta.getEntries().size());

            // Copy key/values from the old map to the new map, applying deltas as we go
            for (Map.Entry<String, ?> entry : map.entrySet()) {
                String key = entry.getKey();
                Object oldValue = entry.getValue();
                Delta valueDelta = delta.getEntries().get(key);
                if (valueDelta != null) {
                    update(result, key, oldValue, valueDelta);
                } else if (!delta.getRemoveRest()) {
                    result.put(key, oldValue);
                }
            }

            // Add new key/values to the map, skipping keys we processed in the previous loop
            for (Map.Entry<String, Delta> entry : delta.getEntries().entrySet()) {
                String key = entry.getKey();
                if (!map.containsKey(key)) {
                    update(result, key, UNDEFINED, entry.getValue());
                }
            }

        } else {
            // Optimize for the case where there's no source map
            result = new HashMap<>(delta.getEntries().size());
            for (Map.Entry<String, Delta> entry : delta.getEntries().entrySet()) {
                update(result, entry.getKey(), UNDEFINED, entry.getValue());
            }
        }

        return delta.getDeleteIfEmpty() && result.isEmpty() ? UNDEFINED : result;
    }

    private void update(Map<Object, Object> map, String key, Object value, Delta delta) {
        Object newValue = delta.visit(this, value);
        if (newValue != UNDEFINED) {
            map.put(key, newValue);
        }
    }

    @Nullable
    @Override
    public Object visit(SetDelta delta, @Nullable Object json) {
        SortedSet<Literal> resultSet;

        // JSON can only represent sets as lists
        if (!delta.getRemoveRest() && json instanceof List) {
            resultSet = new TreeSet<>();
            // Add all added values
            resultSet.addAll(delta.getAddedValues());

            // Copy over existing values that weren't explicitly removed
            Set<Literal> removed = delta.getRemovedValues();
            // noinspection unchecked
            for (Object existing : (List<Object>) json) {
                Literal literal = Deltas.literal(existing);
                if (!removed.contains(literal)) {
                    resultSet.add(literal);
                }
            }
        } else {
            // Existing value is undefined, not a list or "remove rest" was set.  Create the set from all added values
            resultSet = new TreeSet<>(delta.getAddedValues());
        }

        if (delta.getDeleteIfEmpty() && resultSet.isEmpty()) {
            return UNDEFINED;
        }

        List<Object> result = new ArrayList<>(resultSet.size());
        for (Literal literal : resultSet) {
            result.add(literal.getValue());
        }
        return result;
    }

    @Override
    public Object visit(ConditionalDelta delta, @Nullable Object json) {
        boolean test = ConditionEvaluator.eval(delta.getTest(), json, _intrinsics);
        return (test ? delta.getThen() : delta.getElse()).visit(this, json);
    }
}
