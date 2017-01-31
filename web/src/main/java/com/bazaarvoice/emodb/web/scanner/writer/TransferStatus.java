package com.bazaarvoice.emodb.web.scanner.writer;

/**
 * POJO for the current status of a ScanWriter file transfer.
 */
public class TransferStatus implements Cloneable {
    private final ShardMetadata _metadata;
    private final long _size;
    private final int _attempts;
    private final long _bytesTransferred;

    public TransferStatus(ShardMetadata metadata, long size, int attempts, long bytesTransferred) {
        _metadata = metadata;
        _size = size;
        _attempts = attempts;
        _bytesTransferred = bytesTransferred;
    }

    public ShardMetadata getMetadata() {
        return _metadata;
    }

    public long getSize() {
        return _size;
    }

    public int getAttempts() {
        return _attempts;
    }

    public long getBytesTransferred() {
        return _bytesTransferred;
    }
}
