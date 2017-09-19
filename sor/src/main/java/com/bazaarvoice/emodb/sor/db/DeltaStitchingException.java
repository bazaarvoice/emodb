package com.bazaarvoice.emodb.sor.db;

public class DeltaStitchingException extends RuntimeException {

    public DeltaStitchingException() {
        super("Found fragmented deltas without a compaction record ahead of them.");
    }
}