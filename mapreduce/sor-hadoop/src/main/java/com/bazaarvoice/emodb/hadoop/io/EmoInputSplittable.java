package com.bazaarvoice.emodb.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

/**
 * Implemented by {@link EmoFileSystem} and {@link StashFileSystem} to provide a uniform method for getting splits
 * and record readers.
 */
public interface EmoInputSplittable {

    /**
     * Gets the splits for the table located at the given path.
     */
    List<SplitPath> getInputSplits(Configuration config, Path path, int splitSize)
            throws IOException;

    /**
     * Gets a record reader for the split located at the given path.
     */
    BaseRecordReader getBaseRecordReader(Configuration config, Path path, int splitSize)
            throws IOException;
}
