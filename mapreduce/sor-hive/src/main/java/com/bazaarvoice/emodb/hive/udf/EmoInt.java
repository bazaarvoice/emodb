package com.bazaarvoice.emodb.hive.udf;

import com.fasterxml.jackson.core.JsonParser;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

@UDFType (deterministic = true)
@Description (name = "emo_int", value = "Gets the integer value from an EmoDB entry JSON")
public class EmoInt extends AbstractEmoFieldUDF<IntWritable> {

    public IntWritable evaluate(final Text json, final Text field) {
        return evaluateAndNarrow(json, field);
    }

    @Override
    protected IntWritable narrow(JsonParser parser)
            throws IOException {
        return narrowToInt(parser);
    }
}
