package com.bazaarvoice.emodb.hive.udf;

import com.fasterxml.jackson.core.JsonParser;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

@UDFType (deterministic = true)
@Description (name = "emo_boolean", value = "Gets the boolean value from an EmoDB entry JSON")
public class EmoBoolean extends AbstractEmoFieldUDF<BooleanWritable> {

    public BooleanWritable evaluate(final Text json, final Text field) {
        return evaluateAndNarrow(json, field);
    }

    @Override
    protected BooleanWritable narrow(JsonParser parser)
            throws IOException {
        return narrowToBoolean(parser);
    }
}
