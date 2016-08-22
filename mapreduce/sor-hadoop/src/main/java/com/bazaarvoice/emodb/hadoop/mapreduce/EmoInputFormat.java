package com.bazaarvoice.emodb.hadoop.mapreduce;

import com.bazaarvoice.emodb.hadoop.io.BaseInputFormat;
import com.bazaarvoice.emodb.hadoop.io.LocationUtil;
import com.bazaarvoice.emodb.hadoop.io.Row;
import com.bazaarvoice.emodb.hadoop.io.SplitPath;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * EmoDB InputFormat implementation using the newer "mapreduce" libraries.  Note that most of the work is delegated
 * to BaseInputFormat.
 */
public class EmoInputFormat extends FileInputFormat<Text, Row> {

    public static void addInputTable(Job job, String source, String table)
            throws IOException {
        addInputTable(job, LocationUtil.toLocation(source, table).toString());
    }

    public static void addInputTable(Job job, String location)
            throws IOException {
        FileInputFormat.addInputPath(job, new Path(location));
    }

    public static void setInputTable(Job job, String source, String table)
            throws IOException {
        setInputTable(job, LocationUtil.toLocation(source, table).toString());
    }

    public static void setInputTable(Job job, String location)
            throws IOException {
        FileInputFormat.setInputPaths(job, new Path(location));
    }

    public static void setInputTables(Job job, final String source, String... tables)
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

    public static void setInputTables(Job job, final String... locations)
            throws IOException {
        Path[] paths = new Path[locations.length];
        for (int i=0; i < locations.length; i++) {
            paths[i] = new Path(locations[i]);
        }
        FileInputFormat.setInputPaths(job, paths);
    }

    private static final Function<SplitPath, InputSplit> _fromSplit = new Function<SplitPath, InputSplit>() {
        @Override
        public FileSplit apply(SplitPath split) {
            return new FileSplit(split.getPath(), 0, split.getLength(), new String[0]);
        }
    };

    @Override
    public List<InputSplit> getSplits(JobContext context)
            throws IOException {
        Path[] paths = FileInputFormat.getInputPaths(context);

        return FluentIterable.from(BaseInputFormat.getSplits(context.getConfiguration(), paths))
                .transform(_fromSplit)
                .toList();
    }

    @Override
    public RecordReader<Text, Row> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException {
        FileSplit fileSplit  = (FileSplit) split;
        Path path = fileSplit.getPath();
        return new EmoRecordReader(BaseInputFormat.createRecordReader(context.getConfiguration(), path));
    }
}
