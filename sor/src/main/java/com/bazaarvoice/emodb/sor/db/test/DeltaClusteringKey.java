package com.bazaarvoice.emodb.sor.db.test;

import java.util.Objects;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DeltaClusteringKey {

    private final UUID _changeId;
    private final int _numBlocks;

    public DeltaClusteringKey(UUID changeId, int numBlocks) {
        checkArgument(numBlocks > 0);
        _changeId = checkNotNull(changeId);
        _numBlocks = numBlocks;
    }

    public DeltaClusteringKey(UUID changeId) {
        _changeId = checkNotNull(changeId);
        _numBlocks = 0;
    }

    public UUID getChangeId() {
        return _changeId;
    }

    public int getNumBlocks() {
        if (_numBlocks == 0) {
            throw new IllegalStateException(String.format("ChangeId %s does not have blocking data.", _changeId));
        }
        return _numBlocks;
    }

    public boolean hasNumBlocks() {
        return _numBlocks != 0;
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
        return Objects.hash(_changeId, _numBlocks);
    }

    @Override
    public String toString() {
        return "DeltaClusteringKey{" +
                "_changeId=" + _changeId +
                ", _numBlocks=" + _numBlocks +
                '}';
    }
}
