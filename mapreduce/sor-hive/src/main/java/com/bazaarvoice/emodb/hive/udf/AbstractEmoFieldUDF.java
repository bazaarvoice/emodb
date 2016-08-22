package com.bazaarvoice.emodb.hive.udf;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;

abstract public class AbstractEmoFieldUDF<T extends WritableComparable<?>> extends UDF {

    private final JsonFactory _jsonFactory = new ObjectMapper().getFactory();

    protected T evaluateAndNarrow(final Text json, @Nullable final Text field) {
        return evaluateAndNarrow(json, field != null ? field.toString() : null);
    }

    protected T evaluateAndNarrow(final Text json, @Nullable final String field) {
        try {
            JsonParser parser = _jsonFactory.createJsonParser(json.toString());
            // Move to the first token
            parser.nextToken();

            if (field != null) {
                boolean fieldFound = moveParserToField(parser, field);
                if (!fieldFound) {
                    return null;
                }
            }

            if (parser.getCurrentToken() == null || parser.getCurrentToken() == JsonToken.VALUE_NULL) {
                return null;
            }

            return narrow(parser);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Converts a field to a list of segments.  For example, "source.country.lang" becomes ("source", "country", "lang").
     * Dots are escaped using "\." and slashes are escaped using "\\".
     */
    private List<String> getFieldPath(String field) {
        String[] paths = field.split("(?<!\\\\)\\.");
        ImmutableList.Builder<String> fields = ImmutableList.builder();
        for (String path : paths) {
            fields.add(path.replaceAll("\\\\\\.", ".").replaceAll("\\\\\\\\", "\\\\"));
        }
        return fields.build();
    }

    /**
     * Don't materialize the entire parser content, do a targeted search for the value that matches the path.
     */
    private boolean moveParserToField(JsonParser parser, String path)
            throws IOException {
        List<String> segments = getFieldPath(path);

        for (String segment : segments) {
            if (parser.getCurrentToken() != JsonToken.START_OBJECT) {
                // Always expect the path to be fields in a JSON map
                return false;
            }

            boolean found = false;

            JsonToken currentToken = parser.nextToken();
            while (!found && currentToken != JsonToken.END_OBJECT) {
                if (currentToken != JsonToken.FIELD_NAME) {
                    // This should always be a field.  Something is amiss.
                    throw new IOException("Field not found at expected location");
                }
                String fieldName = parser.getText();
                if (fieldName.equals(segment)) {
                    // Move to the next token, which is the field value
                    found = true;
                    currentToken = parser.nextToken();
                } else {
                    parser.nextValue();
                    currentToken = skipValue(parser);
                }
            }

            if (!found) {
                // Field was not found
                return false;
            }
        }

        // The current location in the parser is the value.
        return true;
    }

    /**
     * Skips to the first token after the object at the current parser token.
     */
    protected JsonToken skipValue(JsonParser parser)
            throws IOException {
        return parser
                .skipChildren()
                .nextToken();
    }

    /**
     * Narrow the Json object at the current parser location to the field's type.
     */
    abstract protected T narrow(JsonParser parser)
            throws IOException;

    protected BooleanWritable narrowToBoolean(JsonParser parser)
            throws IOException {
        switch (parser.getCurrentToken()) {
            case VALUE_TRUE:
                return new BooleanWritable(true);
            case VALUE_FALSE:
                return new BooleanWritable(false);
            case VALUE_NUMBER_INT:
                return new BooleanWritable(parser.getIntValue() != 0);
            case VALUE_NUMBER_FLOAT:
                return new BooleanWritable(parser.getFloatValue() != 0);
            default:
                return null;
        }
    }

    protected IntWritable narrowToInt(JsonParser parser)
            throws IOException {
        switch (parser.getCurrentToken()) {
            case VALUE_NUMBER_INT:
                return new IntWritable(parser.getIntValue());
            case VALUE_NUMBER_FLOAT:
                return new IntWritable((int) parser.getFloatValue());
            default:
                return null;
        }
    }

    protected LongWritable narrowToLong(JsonParser parser)
            throws IOException {
        switch (parser.getCurrentToken()) {
            case VALUE_NUMBER_INT:
                return new LongWritable(parser.getLongValue());
            case VALUE_NUMBER_FLOAT:
                return new LongWritable((long) parser.getFloatValue());
            default:
                return null;
        }
    }

    protected FloatWritable narrowToFloat(JsonParser parser)
            throws IOException {
        switch (parser.getCurrentToken()) {
            case VALUE_NUMBER_INT:
                return new FloatWritable(parser.getIntValue());
            case VALUE_NUMBER_FLOAT:
                return new FloatWritable(parser.getFloatValue());
            default:
                return null;
        }
    }

    protected Text narrowToText(JsonParser parser)
            throws IOException {
        switch (parser.getCurrentToken()) {
            case VALUE_STRING:
            case VALUE_NUMBER_INT:
            case VALUE_NUMBER_FLOAT:
                return new Text(parser.getValueAsString());
            case VALUE_TRUE:
                return new Text(Boolean.TRUE.toString());
            case VALUE_FALSE:
                return new Text(Boolean.FALSE.toString());
            default:
                return null;
        }
    }

    protected TimestampWritable narrowToTimestamp(JsonParser parser)
            throws IOException {
        switch (parser.getCurrentToken()) {
            case VALUE_STRING:
                try {
                    return new TimestampWritable(new Timestamp(JsonHelper.parseTimestamp(parser.getValueAsString()).getTime()));
                } catch (Exception e) {
                    // String wasn't an ISO8601 timestamp
                    return null;
                }
            case VALUE_NUMBER_INT:
                return new TimestampWritable(new Timestamp(parser.getLongValue()));
            case VALUE_NUMBER_FLOAT:
                return new TimestampWritable(new Timestamp((long) parser.getFloatValue()));
            default:
                return null;

        }
    }
}
