package com.bazaarvoice.emodb.hive.udf;

import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.fasterxml.jackson.core.JsonParser;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

@UDFType (deterministic = true)
@Description (name = "emo_first_update_at", value = "Gets the first update at timestamp from an EmoDB entry JSON")
public class EmoFirstUpdateAt extends AbstractEmoFieldUDF<TimestampWritable> {

    public TimestampWritable evaluate(final Text json) {
        return evaluateAndNarrow(json, Intrinsic.FIRST_UPDATE_AT);
    }

    @Override
    protected TimestampWritable narrow(JsonParser parser)
            throws IOException {
        return narrowToTimestamp(parser);
    }
}
