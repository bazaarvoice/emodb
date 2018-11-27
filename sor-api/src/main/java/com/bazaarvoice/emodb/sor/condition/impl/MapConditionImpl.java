package com.bazaarvoice.emodb.sor.condition.impl;

import com.bazaarvoice.emodb.common.json.OrderedJson;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.ConditionVisitor;
import com.bazaarvoice.emodb.sor.condition.MapCondition;
import com.bazaarvoice.emodb.sor.delta.deser.DeltaJson;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
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
        for (Iterator<Map.Entry<String, Condition>> iter = _entries.entrySet().stream().sorted(OrderedJson.ENTRY_COMPARATOR).iterator(); iter.hasNext(); ) {
            Map.Entry<String, Condition> entry = iter.next();
            buf.append(',');
            DeltaJson.append(buf, entry.getKey());
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
