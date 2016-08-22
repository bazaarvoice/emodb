package com.bazaarvoice.emodb.hive.udf;

import com.fasterxml.jackson.core.JsonParser;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

@UDFType (deterministic = true)
@Description (name = "emo_long", value = "Gets the long value from an EmoDB entry JSON")
public class EmoLong extends AbstractEmoFieldUDF<LongWritable> {

    public LongWritable evaluate(final Text json, final Text field) {
        return evaluateAndNarrow(json, field);
    }

    @Override
    protected LongWritable narrow(JsonParser parser)
            throws IOException {
        return narrowToLong(parser);
    }
}
