package com.bazaarvoice.emodb.hadoop.io;

import java.net.URI;

/**
 * POJO that maintains S3 directory information for a Stash location.
 */
public class StashLocation {
    private final String _bucket;
    private final String _path;
    private final boolean _useLatestDirectory;

    public StashLocation(String bucket, String path, boolean useLatestDirectory) {
        _bucket = bucket;
        _path = path;
        _useLatestDirectory = useLatestDirectory;
    }

    public String getBucket() {
        return _bucket;
    }

    public String getPath() {
        return _path;
    }

    public URI getUri() {
        return URI.create(String.format("s3://%s/%s", _bucket, _path));
    }

    public boolean isUseLatestDirectory() {
        return _useLatestDirectory;
    }
}
