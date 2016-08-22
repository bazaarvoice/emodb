package com.bazaarvoice.emodb.web.ddl;

import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

final class CqlTemplate {

    public static CqlTemplate create(String templateResourceName) {
        return new CqlTemplate(templateResourceName);
    }

    private final String _templateResourceName;
    private final Map<String, String> _bindings = Maps.newHashMap();

    private CqlTemplate(String templateResourceName) {
        _templateResourceName = templateResourceName;
    }

    public CqlTemplate withBinding(String key, String value) {
        _bindings.put(key, value);
        return this;
    }

    public CqlTemplate withBindings(Map<String, String> bindings) {
        _bindings.putAll(bindings);
        return this;
    }

    public String toCqlScript() {
        try {
            final InputStream cqlStream = CreateKeyspacesCommand.class.getResourceAsStream(_templateResourceName);
            if (cqlStream == null) {
                throw new IllegalStateException("couldn't find " + _templateResourceName + " in classpath");
            }
            String cql;
            try {
                cql = CharStreams.toString(new InputStreamReader(cqlStream, "UTF-8"));
            } finally {
                Closeables.close(cqlStream, true);
            }
            // replace bindings
            for (Map.Entry<String, String> binding : _bindings.entrySet()) {
                cql = cql.replace("${" + binding.getKey() + "}", binding.getValue());
            }
            return cql;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
