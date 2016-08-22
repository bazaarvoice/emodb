package com.bazaarvoice.emodb.hadoop.mapred;

import com.bazaarvoice.emodb.hadoop.io.BaseRecordReader;
import com.bazaarvoice.emodb.hadoop.io.Row;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

/**
 * EmoDB RecordReader implementation using the older "mapred" libraries.  Note that most of the work is delegated
 * to BaseRecordReader.
 */
public class EmoRecordReader implements RecordReader<Text, Row> {

    private final BaseRecordReader _base;
    private boolean _initialized = false;

    public EmoRecordReader(BaseRecordReader base) {
        _base = base;
    }

    @Override
    public boolean next(Text key, Row value)
            throws IOException {
        if (!_initialized) {
            _base.initialize();
            _initialized = true;
        }
        return _base.setNextKeyValue(key, value);
    }

    @Override
    public Text createKey() {
        return new Text();
    }

    @Override
    public Row createValue() {
        return new Row();
    }

    @Override
    public long getPos()
            throws IOException {
        return _base.getRowsRead();
    }

    @Override
    public float getProgress()
            throws IOException {
        return _base.getProgress();
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
