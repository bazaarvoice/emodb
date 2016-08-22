package com.bazaarvoice.emodb.common.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import io.dropwizard.jackson.AnnotationSensitivePropertyNamingStrategy;
import io.dropwizard.jackson.DiscoverableSubtypeResolver;
import io.dropwizard.jackson.FuzzyEnumModule;
import io.dropwizard.jackson.GuavaExtrasModule;
import io.dropwizard.jackson.LogbackModule;

public class CustomJsonObjectMapperFactory {

    // todo: replace this with Jackson.newObjectMapper(JsonFactory jsonFactory) once we upgrade to Dropwizard v8
    public static ObjectMapper build(JsonFactory jsonFactory) {
        ObjectMapper mapper = new ObjectMapper(jsonFactory);

        mapper.registerModule(new GuavaModule());
        mapper.registerModule(new LogbackModule());
        mapper.registerModule(new GuavaExtrasModule());
        mapper.registerModule(new JodaModule());
        mapper.registerModule(new AfterburnerModule());
        mapper.registerModule(new FuzzyEnumModule());
        mapper.setPropertyNamingStrategy(new AnnotationSensitivePropertyNamingStrategy());
        mapper.setSubtypeResolver(new DiscoverableSubtypeResolver());

        return mapper;
    }
}
