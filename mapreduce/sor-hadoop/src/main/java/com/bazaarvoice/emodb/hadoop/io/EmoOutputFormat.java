package com.bazaarvoice.emodb.hadoop.io;

import com.google.common.base.Charsets;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

/**
 * Simple output format that disregards keys and prints each Row as JSON, one row per line.  Useful for testing
 * to get dumps of EmoDB data.
 */
public class EmoOutputFormat<V> extends FileOutputFormat<V, Row> {
    @Override
    public RecordWriter<V, Row> getRecordWriter(TaskAttemptContext job)
            throws IOException, InterruptedException {

        CompressionCodec codec = null;
        String extension = "";

        if (FileOutputFormat.getCompressOutput(job)) {
            Class<? extends CompressionCodec> codecClass = FileOutputFormat.getOutputCompressorClass(job, GzipCodec.class);
            codec = ReflectionUtils.newInstance(codecClass, job.getConfiguration());
            extension = codec.getDefaultExtension();
        }

        Path outputFile = getDefaultWorkFile(job, extension);
        FileSystem fs = outputFile.getFileSystem(job.getConfiguration());
        FSDataOutputStream fileOut = fs.create(outputFile);

        final PrintWriter out;
        if (codec != null) {
            out = new PrintWriter(new OutputStreamWriter(codec.createOutputStream(fileOut), Charsets.UTF_8));
        } else {
            out = new PrintWriter(new OutputStreamWriter(fileOut, Charsets.UTF_8));
        }

        return new RecordWriter<V, Row>() {
            @Override
            public void write(V key, Row value)
                    throws IOException, InterruptedException {
                out.println(value.getJson());
            }

            @Override
            public void close(TaskAttemptContext context)
                    throws IOException, InterruptedException {
                out.close();
            }
        };
    }
}
