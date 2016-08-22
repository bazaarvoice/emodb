package com.bazaarvoice.emodb.hadoop.mapper;

import com.bazaarvoice.emodb.hadoop.io.Row;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Simple mapper to convert Rows back to JSON.
 */
public class RowToJsonMapper<K> extends Mapper<K, Row, K, Text> {

    private Text _json = new Text();

    public void map(K key, Row row, Context context)
            throws IOException, InterruptedException {
        _json.set(row.getJson());
        context.write(key, _json);
    }
}
