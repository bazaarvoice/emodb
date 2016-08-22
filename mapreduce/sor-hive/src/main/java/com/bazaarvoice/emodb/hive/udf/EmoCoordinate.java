package com.bazaarvoice.emodb.hive.udf;

import com.bazaarvoice.emodb.sor.api.Coordinate;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;

import javax.annotation.Nullable;
import java.io.IOException;

@UDFType (deterministic = true)
@Description (name = "emo_coordinate", value = "Gets the coordinate from an EmoDB entry JSON")
public class EmoCoordinate extends AbstractEmoFieldUDF<Text> {

    public Text evaluate(final Text json) {
        return evaluate(json, null);
    }

    public Text evaluate(final Text json, @Nullable final Text field) {
        return evaluateAndNarrow(json, field);
    }

    @Override
    protected Text narrow(JsonParser parser)
            throws IOException {
        if (parser.getCurrentToken() != JsonToken.START_OBJECT) {
            return null;
        }

        String id = null;
        String table = null;
        JsonToken currentToken = parser.nextToken();

        while ((id == null || table == null) && currentToken != JsonToken.END_OBJECT) {
            if (currentToken != JsonToken.FIELD_NAME) {
                // This should always be a field.  Something is amiss.
                throw new IOException("Field not found at expected location");
            }
            String fieldName = parser.getText();
            currentToken = parser.nextToken();

            if (id == null && fieldName.equals(Intrinsic.ID)) {
                if (currentToken != JsonToken.VALUE_STRING) {
                    return null;
                }
                id = parser.getValueAsString();
                currentToken = parser.nextToken();
            } else if (table == null && fieldName.equals(Intrinsic.TABLE)) {
                if (currentToken != JsonToken.VALUE_STRING) {
                    return null;
                }
                table = parser.getValueAsString();
                currentToken = parser.nextToken();
            } else {
                currentToken = skipValue(parser);
            }
        }

        if (id == null || table == null) {
            return null;
        }

        return new Text(Coordinate.of(table, id).toString());
    }
}
