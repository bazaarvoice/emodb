package com.bazaarvoice.emodb.hive.udf;

import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.fasterxml.jackson.core.JsonParser;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;

import java.io.IOException;

@UDFType (deterministic = true)
@Description (name = "emo_id", value = "Gets the intrinsic ID from an EmoDB entry JSON")
public class EmoId extends AbstractEmoFieldUDF<Text> {

    public Text evaluate(final Text json) {
        return evaluateAndNarrow(json, Intrinsic.ID);
    }

    @Override
    protected Text narrow(JsonParser parser)
            throws IOException {
        return narrowToText(parser);
    }
}
