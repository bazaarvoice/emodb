package com.bazaarvoice.emodb.sor.delta.impl;

import com.bazaarvoice.emodb.common.json.OrderedJson;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.DeltaVisitor;
import com.bazaarvoice.emodb.sor.delta.MapDelta;
import com.bazaarvoice.emodb.sor.delta.deser.DeltaJson;
import com.google.common.base.Objects;
import com.google.common.io.CharStreams;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class MapDeltaImpl extends AbstractDelta implements MapDelta {

    private final boolean _removeRest;
    private final Map<String, Delta> _entries;
    private final boolean _deleteIfEmpty;
    private final boolean _constant;

    public MapDeltaImpl(boolean removeRest, Map<String, Delta> entries, boolean deleteIfEmpty) {
        _removeRest = removeRest;
        _entries = checkNotNull(entries, "entries");
        _deleteIfEmpty = deleteIfEmpty;
        _constant = computeConstant();
    }

    private boolean computeConstant() {
        // A map delta is constant if it just adds constant values and removes everything else
        if (!_removeRest) {
            return false;
        }
        for (Delta delta : _entries.values()) {
            if (!delta.isConstant()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean getRemoveRest() {
        return _removeRest;
    }

    @Override
    public Map<String, Delta> getEntries() {
        return _entries;
    }

    @Override
    public boolean getDeleteIfEmpty() {
        return _deleteIfEmpty;
    }

    @Override
    @Nullable
    public <T, V> V visit(DeltaVisitor<T, V> visitor, @Nullable T context) {
        return visitor.visit(this, context);
    }

    @Override
    public boolean isConstant() {
        return _constant;
    }

    @Override
    public void appendTo(Appendable buf) throws IOException {
        // nested content is sorted so toString() is deterministic.  this is valuable in tests and for
        // comparing hashes of deltas for equality.
        buf.append('{');
        String sep = "";
        if (!_removeRest) {
            buf.append("..");
            sep = ",";
        }
        Writer writer = CharStreams.asWriter(buf);
        for (Iterator<Map.Entry<String, Delta>> iter =  _entries.entrySet().stream().sorted(OrderedJson.ENTRY_COMPARATOR).iterator(); iter.hasNext(); ) {
            Map.Entry<String, Delta> entry = iter.next();
            buf.append(sep);
            sep = ",";
            DeltaJson.write(writer, entry.getKey());
            buf.append(':');
            entry.getValue().appendTo(buf);
        }
        buf.append('}');
        if (_deleteIfEmpty) {
            buf.append('?');
        }
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof MapDelta)) {
            return false;
        }
        MapDelta delta = (MapDelta) obj;
        return _removeRest == delta.getRemoveRest() &&
                _entries.equals(delta.getEntries()) &&
                _deleteIfEmpty == delta.getDeleteIfEmpty();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(3169, _removeRest, _entries, _deleteIfEmpty);
    }
}
