package com.bazaarvoice.emodb.hadoop.io;

import org.apache.hadoop.fs.Path;

/**
 * POJO to represent the path and size for a split.
 */
public class SplitPath {
    private final Path _path;
    private final long _length;

    public SplitPath(Path path, long length) {
        _path = path;
        _length = length;
    }

    public Path getPath() {
        return _path;
    }

    public long getLength() {
        return _length;
    }
}
