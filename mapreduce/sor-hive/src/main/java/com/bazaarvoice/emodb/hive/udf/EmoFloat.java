package com.bazaarvoice.emodb.hive.udf;

import com.fasterxml.jackson.core.JsonParser;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

@UDFType (deterministic = true)
@Description (name = "emo_float", value = "Gets the float value from an EmoDB entry JSON")
public class EmoFloat extends AbstractEmoFieldUDF<FloatWritable> {

    public FloatWritable evaluate(final Text json, final Text field) {
        return evaluateAndNarrow(json, field);
    }

    @Override
    protected FloatWritable narrow(JsonParser parser)
            throws IOException {
        return narrowToFloat(parser);
    }
}
