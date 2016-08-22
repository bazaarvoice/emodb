package com.bazaarvoice.emodb.web.scanner.writer;

/**
 * POJO for the current status of a ScanWriter file transfer.
 */
public class TransferStatus implements Cloneable {
    private final TransferKey _key;
    private final long _size;
    private final int _attempts;
    private final long _bytesTransferred;

    public TransferStatus(TransferKey key, long size, int attempts, long bytesTransferred) {
        _key = key;
        _size = size;
        _attempts = attempts;
        _bytesTransferred = bytesTransferred;
    }

    public TransferKey getKey() {
        return _key;
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
