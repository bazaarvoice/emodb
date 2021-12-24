package com.bazaarvoice.emodb.databus.repl;

import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.google.common.base.Charsets;
import com.google.common.io.BaseEncoding;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.UniformInterfaceException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.Collection;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Jersey client for downloading databus events from a remote data center.
 */
public class ReplicationClient implements ReplicationSource {

    /** Must match the @Path annotation on the ReplicationResource1 class. */
    public static final String SERVICE_PATH = "/busrepl/1";

    private final Client _client;
    private final UriBuilder _replicationSource;
    private final String _apiKey;

    public ReplicationClient(URI endPoint, Client jerseyClient, String apiKey) {
        _client = requireNonNull(jerseyClient, "jerseyClient");
        _replicationSource = UriBuilder.fromUri(endPoint);
        _apiKey = apiKey;
    }

    @Override
    public List<ReplicationEvent> get(String channel, int limit) {
        requireNonNull(channel, "channel");
        try {
            URI uri = _replicationSource.clone()
                    .segment(channel)
                    .queryParam("limit", limit)
                    .build();
            return _client.resource(uri)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                    .get(new GenericType<List<ReplicationEvent>>() {});
        } catch (UniformInterfaceException e) {
            throw convertException(e);
        }
    }

    @Override
    public void delete(String channel, Collection<String> eventIds) {
        requireNonNull(channel, "channel");
        requireNonNull(eventIds, "eventIds");
        try {
            URI uri = _replicationSource.clone()
                    .segment(channel, "ack")
                    .build();
            _client.resource(uri)
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                    .post(eventIds);
        } catch (UniformInterfaceException e) {
            throw convertException(e);
        }
    }

    private RuntimeException convertException(UniformInterfaceException e) {
        ClientResponse response = e.getResponse();
        String exceptionType = response.getHeaders().getFirst("X-BV-Exception");

        if (response.getStatus() == Response.Status.BAD_REQUEST.getStatusCode() &&
                IllegalArgumentException.class.getName().equals(exceptionType)) {
            return new IllegalArgumentException(response.getEntity(String.class), e);
        }
        return e;
    }

    private String basicAuthCredentials(String username, String password) {
        String credentials = String.format("%s:%s", username, password);
        return String.format("Basic %s", BaseEncoding.base64().encode(credentials.getBytes(Charsets.UTF_8)));
    }
}
