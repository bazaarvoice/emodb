package com.bazaarvoice.emodb.hadoop.io;

import com.bazaarvoice.emodb.hadoop.ConfigurationParameters;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Arrays;

/**
 * Common base InputFormat implementation used by both the older "mapred" and newer "mapreduce" implementations of
 * InputFormat.  For Hadoop inheritance reasons neither implementation can directly inherit from this class, although
 * both defer most of their work to it.
 *
 * @see com.bazaarvoice.emodb.hadoop.mapred.EmoInputFormat
 * @see com.bazaarvoice.emodb.hadoop.mapreduce.EmoInputFormat
 */
public class BaseInputFormat {

    // Default split size if not configured
    private static final int DEFAULT_SPLIT_SIZE = 10000;

    private BaseInputFormat() {
        // empty
    }

    /**
     * Gets the split size from the configuration.
     */
    public static int getSplitSize(Configuration config) {
        return config.getInt(ConfigurationParameters.SPLIT_SIZE, DEFAULT_SPLIT_SIZE);
    }

    /**
     * Gets the splits for a given list of EmoDB paths.
     */
    public static Iterable<SplitPath> getSplits(final Configuration config, Path[] paths) {
        final int splitSize = getSplitSize(config);

        return FluentIterable.from(Arrays.asList(paths))
                .transformAndConcat(new Function<Path, Iterable<? extends SplitPath>>() {
                    @Override
                    public Iterable<? extends SplitPath> apply(Path path) {
                        try {
                            EmoInputSplittable emoFs = (EmoInputSplittable) path.getFileSystem(config);
                            return emoFs.getInputSplits(config, path, splitSize);
                        } catch (IOException e) {
                            throw Throwables.propagate(e);
                        }
                    }
                });
    }

    /**
     * Gets a record reader for a given split.
     */
    public static BaseRecordReader createRecordReader(Configuration config, Path path)
            throws IOException {
        EmoInputSplittable emoFs = (EmoInputSplittable) path.getFileSystem(config);
        return emoFs.getBaseRecordReader(config, path, getSplitSize(config));
    }
}
