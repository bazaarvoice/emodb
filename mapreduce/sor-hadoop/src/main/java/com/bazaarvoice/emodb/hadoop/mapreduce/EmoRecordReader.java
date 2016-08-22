package com.bazaarvoice.emodb.hadoop.mapreduce;

import com.bazaarvoice.emodb.hadoop.io.BaseRecordReader;
import com.bazaarvoice.emodb.hadoop.io.Row;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * EmoDB RecordReader implementation using the newer "mapreduce" libraries.  Note that most of the work is delegated
 * to BaseRecordReader.
 */
public class EmoRecordReader extends RecordReader<Text, Row> {

    private final BaseRecordReader _base;
    private final Text _key = new Text();
    private final Row _value = new Row();

    public EmoRecordReader(BaseRecordReader base) {
        _base = base;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        _base.initialize();
    }

    @Override
    public boolean nextKeyValue()
            throws IOException, InterruptedException {
        return _base.setNextKeyValue(_key, _value);
    }

    @Override
    public Text getCurrentKey()
            throws IOException, InterruptedException {
        return _key;
    }

    @Override
    public Row getCurrentValue()
            throws IOException, InterruptedException {
        return _value;
    }

    @Override
    public float getProgress()
            throws IOException, InterruptedException {
        return _base.getRowsRead();
    }

    @Override
    public void close()
            throws IOException {
        _base.close();
    }

    @Override
    protected void finalize()
            throws Throwable {
        super.finalize();
        _base.close();
    }
}
