package com.bazaarvoice.emodb.hive.udf;

import com.fasterxml.jackson.core.JsonParser;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

@UDFType (deterministic = true)
@Description (name = "emo_timestamp", value = "Gets the timestamp from an EmoDB entry JSON")
public class EmoTimestamp extends AbstractEmoFieldUDF<TimestampWritable> {

    public TimestampWritable evaluate(final Text json, final Text field) {
        return evaluateAndNarrow(json, field);
    }

    @Override
    protected TimestampWritable narrow(JsonParser parser)
            throws IOException {
        return narrowToTimestamp(parser);
    }
}
