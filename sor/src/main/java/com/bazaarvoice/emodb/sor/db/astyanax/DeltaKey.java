package com.bazaarvoice.emodb.sor.db.astyanax;

import com.netflix.astyanax.annotations.Component;

import java.util.Objects;
import java.util.UUID;

public class DeltaKey {

    private @Component(ordinal=0) UUID _changeId;
    private @Component(ordinal=1) Integer _block;

    public DeltaKey() {
        // no-op
    }

    public DeltaKey(UUID changeId, Integer block) {
        _changeId = changeId;
        _block = block;
    }

    public UUID getChangeId() {
        return _changeId;
    }

    public Integer getBlock() {
        return _block;
    }

    @Override
    public int hashCode() {
        return Objects.hash(_changeId, _block);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        DeltaKey other = (DeltaKey) obj;
        boolean equal = true;
        equal &= (_changeId != null) ? (_changeId.equals(other.getChangeId())) : other.getChangeId() == null;
        equal &= (_block != null) ? (_block.equals(other.getBlock())) : other.getBlock() == null;
        return equal;
    }

    public DeltaKey clone() {
        return new DeltaKey(_changeId, _block);
    }

    @Override
    public String toString() {
        return String.format("%s/%s", _changeId, _block);
    }
}