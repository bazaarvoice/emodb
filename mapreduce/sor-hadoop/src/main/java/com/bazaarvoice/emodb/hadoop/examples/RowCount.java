package com.bazaarvoice.emodb.hadoop.examples;

import com.bazaarvoice.emodb.hadoop.io.EmoFileSystem;
import com.bazaarvoice.emodb.hadoop.io.Row;
import com.bazaarvoice.emodb.hadoop.io.StashFileSystem;
import com.bazaarvoice.emodb.hadoop.mapreduce.EmoInputFormat;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Simple example MapReduce job which counts the number of rows in a given list of tables.
 *
 * Usage example:
 *
 * java com.bazaarvoice.emodb.hadoop.examples.RowCount emodb://ci.us/review:testcustomer emodb://ci.us/question:testcustomer /path/to/results/output/dir
 */
public class RowCount {

    public static class RowMapper
            extends Mapper<Text, Row, Text, IntWritable>{

        private final static IntWritable _one = new IntWritable(1);
        private Text _tableName = new Text();

        public void map(Text key, Row value, Context context)
                throws IOException, InterruptedException {
            _tableName.set(Intrinsic.getTable(value.getMap()));
            context.write(_tableName, _one);
        }
    }

    public static class CountReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable _count = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }
            _count.set(count);
            context.write(key, _count);
        }
    }

    public static void main(String[] args) throws Exception {
        checkArgument(args.length >= 2, "Input table and output directory required");
        Configuration conf = new Configuration();
        conf.setClass("fs.emodb.impl", EmoFileSystem.class, FileSystem.class);
        conf.setClass("fs.emostash.impl", StashFileSystem.class, FileSystem.class);
        Job job = Job.getInstance(conf, "row count");
        job.setJarByClass(RowCount.class);
        job.setMapperClass(RowMapper.class);
        job.setCombinerClass(CountReducer.class);
        job.setReducerClass(CountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(EmoInputFormat.class);
        for (int i=0; i < args.length-1; i++) {
            EmoInputFormat.addInputTable(job, args[i]);
        }
        FileOutputFormat.setOutputPath(job, new Path(args[args.length-1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}