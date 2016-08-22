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
import java.util.Map;

public class MapConditionImpl extends AbstractCondition implements MapCondition {

    private final Map<String, Condition> _entries;

    public MapConditionImpl(Map<String, Condition> entries) {
        _entries = entries;
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

    @Override
    public boolean equals(Object o) {
        return (this == o) || (o instanceof MapCondition) && _entries.equals(((MapCondition) o).getEntries());
    }

    @Override
    public int hashCode() {
        return 62131 ^ _entries.hashCode();
    }
}
