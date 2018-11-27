package com.bazaarvoice.emodb.sor.delta.impl;

import com.bazaarvoice.emodb.sor.delta.DeltaVisitor;
import com.bazaarvoice.emodb.sor.delta.Literal;
import com.bazaarvoice.emodb.sor.delta.SetDelta;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class SetDeltaImpl extends AbstractDelta implements SetDelta {

    private final boolean _removeRest;
    private final Set<Literal> _addedValues;
    private final Set<Literal> _removedValues;
    private final boolean _deleteIfEmpty;

    public SetDeltaImpl(boolean removeRest, Collection<Literal> addedValues, Collection<Literal> removedValues, boolean deleteIfEmpty) {
        _removeRest = removeRest;
        _addedValues = sorted(requireNonNull(addedValues, "addedValues"));
        _removedValues = sorted(requireNonNull(removedValues, "removedValues"));
        _deleteIfEmpty = deleteIfEmpty;
    }

    @Override
    public boolean getRemoveRest() {
        return _removeRest;
    }

    @Override
    public Set<Literal> getAddedValues() {
        return _addedValues;
    }

    @Override
    public Set<Literal> getRemovedValues() {
        return _removedValues;
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
        // A set can only contain constant literals, so it is constant as long as it completely overwrites any prior delta.
        return _removeRest;
    }

    @Override
    public void appendTo(Appendable buf) throws IOException {
        buf.append('(');
        String sep = "";
        if (!_removeRest) {
            buf.append("..");
            sep = ",";
        }

        // Append all additions followed by all removals
        sep = appendLiterals(buf, _addedValues, sep, "");
        appendLiterals(buf, _removedValues, sep, "~");

        buf.append(')');
        if (_deleteIfEmpty) {
            buf.append('?');
        }
    }

    private Set<Literal> sorted(Collection<Literal> literals) {
        // Optimize the simple cases
        switch (literals.size()) {
            case 0:
                return Collections.emptySet();
            case 1:
                return Collections.unmodifiableSet(new LinkedHashSet<>(literals));
        }

        Set<Literal> sortedSet = literals.stream().sorted().collect(Collectors.toCollection(TreeSet::new));
        return Collections.unmodifiableSet(sortedSet);
    }

    private String appendLiterals(Appendable buf, Set<Literal> literals, String sep, String prefix)
            throws IOException {
        if (literals.isEmpty()) {
            return sep;
        }

        // Literals are already deterministically sorted, so they can be appended in order
        for (Literal literal : literals) {
            buf.append(sep);
            sep = ",";
            buf.append(prefix);
            literal.appendTo(buf);
        }

        return sep;
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SetDelta)) {
            return false;
        }
        SetDelta delta = (SetDelta) obj;
        return _removeRest == delta.getRemoveRest() &&
                _addedValues.equals(delta.getAddedValues()) &&
                _removedValues.equals(delta.getRemovedValues()) &&
                _deleteIfEmpty == delta.getDeleteIfEmpty();
    }

    @Override
    public int hashCode() {
        return Objects.hash(9532, _removeRest, _addedValues, _removedValues, _deleteIfEmpty);
    }
}
