package com.bazaarvoice.emodb.hadoop.mapper;

import com.bazaarvoice.emodb.hadoop.io.Row;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Simple mapper to convert pure JSON to Rows.
 */
public class JsonToRowMapper<Key> extends Mapper<Key, Text, Key, Row> {

    private Row _row = new Row();

    public void map(Key key, Text json, Context context)
            throws IOException, InterruptedException {
        _row.set(json.toString());
        context.write(key, _row);
    }
}
