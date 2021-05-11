package com.bazaarvoice.emodb.common.json;

import com.bazaarvoice.jackson.rison.RisonFactory;
import com.bazaarvoice.jackson.rison.RisonGenerator;
import com.bazaarvoice.jackson.rison.RisonParser;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.io.Reader;

/**
 * Helper for the Rison variant of JSON described at http://mjtemplate.org/examples/rison.html.
 */
public class RisonHelper {

    /**
     * Use an extension of the Jackson JSON parser to parse Rison.  Configure this ObjectMapper to use the "O-Rison"
     * variant where every object is assumed to be an Object (a map) so the surrounding parenthesis '(', ')' are
     * omitted.
     */
    private static final ObjectMapper O_RISON = CustomJsonObjectMapperFactory.build(new RisonFactory() {
        @Override
        public boolean canUseCharArrays() {
            return false;
        }


        @Override
        protected RisonParser _createParser(Reader r, IOContext ctxt)
                throws IOException, JsonParseException {
            return new RisonParser(ctxt, _parserFeatures, _risonParserFeatures, r, _objectCodec,
                    _rootCharSymbols.makeChild(
                            (isEnabled(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES) ? JsonFactory.Feature.CANONICALIZE_FIELD_NAMES.getMask() : 0)
                                    |
                                    (isEnabled(JsonFactory.Feature.INTERN_FIELD_NAMES) ? JsonFactory.Feature.INTERN_FIELD_NAMES.getMask() : 0)));
        }
    }.enable(RisonGenerator.Feature.O_RISON)
            .enable(RisonParser.Feature.O_RISON));

    public static String asORison(Object value) {
        try {
            return O_RISON.writeValueAsString(value);
        } catch (IOException e) {
            // Shouldn't get I/O errors writing to a string.
            throw Throwables.propagate(e);
        }
    }

    public static <T> T fromORison(String string, Class<T> valueType) {
        try {
            return O_RISON.readValue(string, valueType);
        } catch (IOException e) {
            // Must be malformed O-Rison.  Other kinds of I/O errors don't get thrown when reading from a string.
            throw new IllegalArgumentException(e.toString());
        }
    }
}
