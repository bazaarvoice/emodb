package com.bazaarvoice.emodb.cachemgr.invalidate;

import com.bazaarvoice.emodb.cachemgr.api.InvalidationEvent;
import com.bazaarvoice.emodb.cachemgr.api.InvalidationScope;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Throwables;
import com.google.common.net.HttpHeaders;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import java.time.Duration;

/**
 * Simple HTTP client that invokes the {@link DropwizardInvalidationTask} in other JVMs.
 */
public class DropwizardInvalidationClient implements RemoteInvalidationClient {
    private static final Logger _log = LoggerFactory.getLogger(DropwizardInvalidationClient.class);

    private static final int NUM_ATTEMPTS = 10;
    private static final Duration SLEEP_BETWEEN_RETRY = Duration.ofSeconds(1);

    private final Client _client;

    @Inject
    public DropwizardInvalidationClient(Client client) {
        _client = client;
    }

    @Timed(name = "bv.emodb.cachemgr.DropwizardInvalidationClient.invalidateAll", absolute = true)
    @Override
    public void invalidateAll(String invalidateUrl, InvalidationScope scope, InvalidationEvent event) {
        // Use Jersey to make an HTTP request to the server
        WebTarget invalidateResource = _client.target(invalidateUrl);
        Form form = new Form();
        form.param("cache", event.getCache());
        form.param("scope", scope.name().toLowerCase());
        if (event.hasKeys()) {
            event.getKeys().forEach(key -> form.param("key", key));
        }

        _log.debug("Invalidating cache at url {} with params: {}", invalidateUrl, form);

        int numAttempts = 0;
        for (;;) {
            numAttempts++;
            try {
                // Execute the HTTP request
                invalidateResource.request()
                        .header(HttpHeaders.CONNECTION, "close")
                        .post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE));
                return; // Success
            } catch (RuntimeException e) {
                if (numAttempts >= NUM_ATTEMPTS) {
                    throw e;
                }
            }
            try {
                Thread.sleep(SLEEP_BETWEEN_RETRY.toMillis());
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
