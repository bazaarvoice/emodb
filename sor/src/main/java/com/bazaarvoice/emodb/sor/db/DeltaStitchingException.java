package com.bazaarvoice.emodb.sor.db;

public class DeltaStitchingException extends RuntimeException {

    public DeltaStitchingException(String rowkey, String changeId) {
        super(String.format("Found fragmented deltas without a compaction record ahead of them.\nrowkey=%s changeid=%s", rowkey, changeId));
    }
}