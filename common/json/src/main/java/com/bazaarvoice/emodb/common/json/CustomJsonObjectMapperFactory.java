package com.bazaarvoice.emodb.common.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JSR310Module;

import javax.annotation.Nullable;

public class CustomJsonObjectMapperFactory {

    // todo: replace this with Jackson.newObjectMapper(JsonFactory jsonFactory) once we upgrade to Dropwizard v8
    public static ObjectMapper build() {
        return build(null);
    }

    public static ObjectMapper build(@Nullable JsonFactory jsonFactory) {
        return configure(new ObjectMapper(jsonFactory));
    }

    public static ObjectMapper configure(ObjectMapper mapper) {
        return mapper
                .registerModule(new Jdk8Module())
                .registerModule(new JSR310Module())
                .configure(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS, false)
                // The following module present to maintain compatibility in the API clients caused by importing DropWizard.
                // Eventually all API clients should have minimal dependencies. At that time Guava will be removed and the
                // JSON support for Guava objects will also be removed.
                .registerModule(new GuavaModule());
    }
}
