package com.bazaarvoice.emodb.web.util;

import com.bazaarvoice.emodb.common.json.CustomJsonObjectMapperFactory;
import com.bazaarvoice.emodb.common.json.ISO8601DateFormat;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.jackson.AnnotationSensitivePropertyNamingStrategy;
import io.dropwizard.jackson.DiscoverableSubtypeResolver;
import io.dropwizard.jackson.LogbackModule;

import javax.annotation.Nullable;

/**
 * Like {@link CustomJsonObjectMapperFactory} but with additional modules and configurations needed specifically for the
 * web server.
 */
public class EmoServiceObjectMapperFactory {

    public static ObjectMapper build() {
        return build(null);
    }

    public static ObjectMapper build(@Nullable JsonFactory jsonFactory) {
        return configure(new ObjectMapper(jsonFactory));
    }

    public static ObjectMapper configure(ObjectMapper mapper) {
        return CustomJsonObjectMapperFactory.configure(mapper)
                .registerModule(new LogbackModule())
                .setDateFormat(new ISO8601DateFormat())
                .setPropertyNamingStrategy(new AnnotationSensitivePropertyNamingStrategy())
                .setSubtypeResolver(new DiscoverableSubtypeResolver());

    }
}
