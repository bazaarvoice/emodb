package com.bazaarvoice.emodb.sor.core;

import java.time.Instant;

public class DataStoreMinSplitSize {
    private final int _minSplitSize;
    private final Instant _expirationTime;

    public DataStoreMinSplitSize(int minSplitSize, Instant expirationTime) {
        _minSplitSize = minSplitSize;
        _expirationTime = expirationTime;
    }

    public int getMinSplitSize() {
        return _minSplitSize;
    }

    public Instant getExpirationTime() {
        return _expirationTime;
    }
}
