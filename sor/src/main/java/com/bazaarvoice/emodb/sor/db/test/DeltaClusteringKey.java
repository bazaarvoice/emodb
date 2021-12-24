package com.bazaarvoice.emodb.sor.db.test;

import java.util.Objects;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;

public class DeltaClusteringKey {

    private final UUID _changeId;
    private final int _numBlocks;

    public DeltaClusteringKey(UUID changeId, int numBlocks) {
        checkArgument(numBlocks > 0);
        _changeId = requireNonNull(changeId);
        _numBlocks = numBlocks;
    }

    public UUID getChangeId() {
        return _changeId;
    }

    public int getNumBlocks() {
        return _numBlocks;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeltaClusteringKey that = (DeltaClusteringKey) o;
        return _numBlocks == that.getNumBlocks() &&
                Objects.equals(_changeId, that.getChangeId());
    }

    @Override
    public int hashCode() {
        return hash(_changeId, _numBlocks);
    }

    @Override
    public String toString() {
        return "DeltaClusteringKey{" +
                "_changeId=" + _changeId +
                ", _numBlocks=" + _numBlocks +
                '}';
    }
}
