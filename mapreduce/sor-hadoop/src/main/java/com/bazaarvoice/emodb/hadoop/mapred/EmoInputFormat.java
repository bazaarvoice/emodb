package com.bazaarvoice.emodb.hadoop.mapred;

import com.bazaarvoice.emodb.hadoop.io.BaseInputFormat;
import com.bazaarvoice.emodb.hadoop.io.LocationUtil;
import com.bazaarvoice.emodb.hadoop.io.Row;
import com.bazaarvoice.emodb.hadoop.io.SplitPath;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Arrays;

/**
 * EmoDB InputFormat implementation using the older "mapred" libraries.  Note that most of the work is delegated
 * to BaseInputFormat.
 */
public class EmoInputFormat extends FileInputFormat<Text, Row> {

    public static void addInputTable(JobConf job, String source, String table)
            throws IOException {
        addInputTable(job, LocationUtil.toLocation(source, table).toString());
    }

    public static void addInputTable(JobConf job, String location)
            throws IOException {
        addInputPath(job, new Path(location));
    }

    public static void setInputTable(JobConf job, String source, String table)
            throws IOException {
        setInputTable(job, LocationUtil.toLocation(source, table).toString());
    }

    public static void setInputTable(JobConf job, String location)
            throws IOException {
        setInputPaths(job, new Path(location));
    }

    public static void setInputTables(JobConf job, final String source, String... tables)
            throws IOException {
        setInputTables(job,
                FluentIterable
                        .from(Arrays.asList(tables))
                        .transform(new Function<String, String>() {
                            @Override
                            public String apply(String table) {
                                return LocationUtil.toLocation(source, table).toString();
                            }
                        })
                        .toArray(String.class));
    }

    public static void setInputTables(JobConf job, final String... locations)
            throws IOException {
        Path[] paths = new Path[locations.length];
        for (int i=0; i < locations.length; i++) {
            paths[i] = new Path(locations[i]);
        }
        setInputPaths(job, paths);
    }

    private static final Function<SplitPath, InputSplit> _fromSplit = new Function<SplitPath, InputSplit>() {
        @Override
        public FileSplit apply(SplitPath split) {
            return new FileSplit(split.getPath(), 0, split.getLength(), new String[0]);
        }
    };

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits)
            throws IOException {
        Path[] paths = FileInputFormat.getInputPaths(job);

        return FluentIterable.from(BaseInputFormat.getSplits(job, paths))
                .transform(_fromSplit)
                .toArray(InputSplit.class);
    }

    @Override
    public RecordReader<Text, Row> getRecordReader(InputSplit split, JobConf config, Reporter reporter)
            throws IOException {
        FileSplit fileSplit = (FileSplit) split;
        Path path = fileSplit.getPath();
        return new EmoRecordReader(BaseInputFormat.createRecordReader(config, path));
    }
}
