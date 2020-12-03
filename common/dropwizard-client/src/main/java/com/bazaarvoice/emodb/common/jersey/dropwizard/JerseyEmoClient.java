package com.bazaarvoice.emodb.common.jersey.dropwizard;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.client.EmoResource;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import org.glassfish.jersey.logging.LoggingFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.Feature;
import java.net.URI;
import java.util.logging.Level;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * EmoClient implementation that uses a Jersey client.
 */
public class JerseyEmoClient implements EmoClient {

    private static final Logger LOG = LoggerFactory.getLogger(JerseyEmoClient.class);

    private final Client _client;

    public JerseyEmoClient(Client client) {
        _client = checkNotNull(client, "client");

        Feature feature = new LoggingFeature(new LoggerProxy(LOG), Level.FINE, LoggingFeature.Verbosity.PAYLOAD_ANY,
            LoggingFeature.DEFAULT_MAX_ENTITY_SIZE);

        _client.register(JacksonJsonProvider.class);
        _client.register(feature);
    }

    @Override
    public EmoResource resource(URI uri) {
        return new JerseyEmoResource(_client.target(uri));
    }

    private static class LoggerProxy extends java.util.logging.Logger {

        private final Logger delegate;

        LoggerProxy(org.slf4j.Logger delegate) {
            super(delegate.getName(), null);
            this.delegate = delegate;
        }

        @Override public void info(String msg) {
            delegate.info(msg);
        }

        @Override public void warning(String msg) {
            delegate.warn(msg);
        }

        @Override public void severe(String msg) {
            delegate.error(msg);
        }

        @Override public void fine(String msg) {
            delegate.debug(msg);
        }

        @Override public void finer(String msg) {
            delegate.debug(msg);
        }

        @Override public void finest(String msg) {
            delegate.trace(msg);
        }

        private LoggerProxy(final String name, final String resourceBundleName) {
            super(name, resourceBundleName);

            this.delegate = null;
        }
    }
}
