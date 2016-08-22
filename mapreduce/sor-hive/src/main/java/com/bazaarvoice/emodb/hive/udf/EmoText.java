package com.bazaarvoice.emodb.hive.udf;

import com.fasterxml.jackson.core.JsonParser;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;

import java.io.IOException;

@UDFType (deterministic = true)
@Description (name = "emo_text", value = "Gets the text from an EmoDB entry JSON")
public class EmoText extends AbstractEmoFieldUDF<Text> {

    public Text evaluate(final Text json, final Text field) {
        return evaluateAndNarrow(json, field);
    }

    @Override
    protected Text narrow(JsonParser parser)
            throws IOException {
        return narrowToText(parser);
    }
}
