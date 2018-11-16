package com.bazaarvoice.emodb.sor.condition.impl;

import com.bazaarvoice.emodb.common.json.OrderedJson;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.ConditionVisitor;
import com.bazaarvoice.emodb.sor.condition.MapCondition;
import com.bazaarvoice.emodb.sor.delta.deser.DeltaJson;
import com.google.common.io.CharStreams;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Writer;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Standard implementation for {@link MapCondition}.  Note that this implementation does not preserve the original order
 * of the conditions in the provided map but sorts them such that {@link #getEntries()} is sorted by increasing weight.
 * The serialization performed by {@link #appendTo(Appendable)} always outputs entries in order sorted by map keys.
 */
public class MapConditionImpl extends AbstractCondition implements MapCondition {

    private final Map<String, Condition> _entries;

    public MapConditionImpl(Map<String, Condition> entries) {
        // Use a LinkedHashMap to provide a map interface which returns the conditions in weighted order.
        final Map<String, Condition> sortedEntries = new LinkedHashMap<>();
        entries.entrySet().stream()
                .sorted(Comparator.comparingInt(entry -> entry.getValue().weight()))
                .forEach(entry -> sortedEntries.put(entry.getKey(), entry.getValue()));
        _entries = Collections.unmodifiableMap(sortedEntries);
    }

    @Override
    public Map<String, Condition> getEntries() {
        return _entries;
    }

    @Override
    public <T, V> V visit(ConditionVisitor<T, V> visitor, @Nullable T context) {
        return visitor.visit(this, context);
    }

    @Override
    public void appendTo(Appendable buf) throws IOException {
        buf.append("{..");
        Writer writer = CharStreams.asWriter(buf);
        for (Map.Entry<String, Condition> entry : OrderedJson.ENTRY_COMPARATOR.immutableSortedCopy(_entries.entrySet())) {
            buf.append(',');
            DeltaJson.write(writer, entry.getKey());
            buf.append(':');
            entry.getValue().appendTo(buf);
        }
        buf.append('}');
    }

    /**
     * The worst case total weight of a "map" is the sum of the weights of all contained conditions.
     */
    @Override
    public int weight() {
        return _entries.values().stream().mapToInt(Condition::weight).sum();
    }

    @Override
    public boolean equals(Object o) {
        return (this == o) || (o instanceof MapCondition) && _entries.equals(((MapCondition) o).getEntries());
    }

    @Override
    public int hashCode() {
        return 62131 ^ _entries.hashCode();
    }
}
