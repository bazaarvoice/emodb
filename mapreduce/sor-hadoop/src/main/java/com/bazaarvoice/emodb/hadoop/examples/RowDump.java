package com.bazaarvoice.emodb.hadoop.examples;

import com.bazaarvoice.emodb.hadoop.io.EmoFileSystem;
import com.bazaarvoice.emodb.hadoop.io.EmoOutputFormat;
import com.bazaarvoice.emodb.hadoop.io.Row;
import com.bazaarvoice.emodb.hadoop.io.StashFileSystem;
import com.bazaarvoice.emodb.hadoop.mapreduce.EmoInputFormat;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Simple example MapReduce job which dumps the contents of given list of tables.
 *
 * Usage example:
 *
 * java com.bazaarvoice.emodb.hadoop.examples.RowDump emodb://ci.us/review:testcustomer emodb://ci.us/question:testcustomer /path/to/results/output/dir
 */
public class RowDump {

    public static class RowMapper
            extends Mapper<Text, Row, Text, Row>{

        private Text _tableName = new Text();

        public void map(Text key, Row value, Context context)
                throws IOException, InterruptedException {
            _tableName.set(Intrinsic.getTable(value.getMap()));
            context.write(_tableName, value);
        }
    }

    public static void main(String[] args) throws Exception {
        checkArgument(args.length >= 2, "Input table and output directory required");
        Configuration conf = new Configuration();
        conf.setClass("fs.emodb.impl", EmoFileSystem.class, FileSystem.class);
        conf.setClass("fs.emostash.impl", StashFileSystem.class, FileSystem.class);
        Job job = Job.getInstance(conf, "row count");
        job.setJarByClass(RowDump.class);
        job.setMapperClass(RowMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Row.class);
        job.setInputFormatClass(EmoInputFormat.class);
        job.setOutputFormatClass(EmoOutputFormat.class);
        for (int i=0; i < args.length-1; i++) {
            EmoInputFormat.addInputTable(job, args[i]);
        }
        FileOutputFormat.setOutputPath(job, new Path(args[args.length-1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}