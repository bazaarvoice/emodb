package com.bazaarvoice.emodb.databus.repl;

import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

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
        _client = checkNotNull(jerseyClient, "jerseyClient");
        _replicationSource = UriBuilder.fromUri(endPoint);
        _apiKey = apiKey;
    }

    @Override
    public List<ReplicationEvent> get(String channel, int limit) {
        checkNotNull(channel, "channel");
        try {
            URI uri = _replicationSource.clone()
                    .segment(channel)
                    .queryParam("limit", limit)
                    .build();
            return _client.target(uri)
                    .request()
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                    .get(new GenericType<List<ReplicationEvent>>() {});
        } catch (WebApplicationException e) {
            throw convertException(e);
        }
    }

    @Override
    public void delete(String channel, Collection<String> eventIds) {
        checkNotNull(channel, "channel");
        checkNotNull(eventIds, "eventIds");
        try {
            URI uri = _replicationSource.clone()
                    .segment(channel, "ack")
                    .build();
            Response response = _client.target(uri)
                    .request()
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                    .post(Entity.entity(eventIds, MediaType.APPLICATION_JSON_TYPE));

            if (!response.getStatusInfo().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                throw new WebApplicationException(response);
            }
        } catch (WebApplicationException e) {
            throw convertException(e);
        }
    }

    private RuntimeException convertException(WebApplicationException e) {
        Response response = e.getResponse();
        String exceptionType = response.getStringHeaders().getFirst("X-BV-Exception");

        if (response.getStatus() == Response.Status.BAD_REQUEST.getStatusCode() &&
                IllegalArgumentException.class.getName().equals(exceptionType)) {
            return new IllegalArgumentException(response.readEntity(String.class), e);
        }
        return e;
    }

}
