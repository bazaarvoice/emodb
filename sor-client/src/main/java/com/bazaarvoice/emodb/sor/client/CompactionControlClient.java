package com.bazaarvoice.emodb.sor.client;

import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.api.StashRunTimeInfo;
import com.bazaarvoice.emodb.sor.api.StashTimeKey;
import com.google.common.base.Preconditions;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class CompactionControlClient implements CompactionControlSource {

    private final EmoClient _client;
    private final UriBuilder _compactionControlSource;
    private final String _apiKey;

    public CompactionControlClient(URI endPoint, EmoClient jerseyClient, String apiKey) {
        _client = Preconditions.checkNotNull(jerseyClient, "jerseyClient");
        _compactionControlSource = UriBuilder.fromUri(endPoint);
        _apiKey = Preconditions.checkNotNull(apiKey, "apiKey");
    }

    @Override
    public void updateStashTime(String id, long timestamp, List<String> placements, long expiredTimestamp, String dataCenter) {
        checkNotNull(id, "id");
        checkNotNull(placements, "placements");
        checkNotNull(dataCenter, "dataCenter");

        try {
            UriBuilder uriBuilder = _compactionControlSource.clone()
                    .segment("_compcontrol", "stash-time", id)
                    .queryParam("timestamp", timestamp)
                    .queryParam("expiredTimestamp", expiredTimestamp)
                    .queryParam("dataCenter", dataCenter);
            for (String placement : placements) {
                uriBuilder.queryParam("placement", placement);
            }
            URI uri = uriBuilder.build();
            _client.resource(uri)
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                    .post();
        } catch (UniformInterfaceException e) {
            throw convertException(e);
        }
    }

    @Override
    public void deleteStashTime(String id, String dataCenter) {
        checkNotNull(id, "id");
        checkNotNull(dataCenter, "dataCenter");

        try {
            URI uri = _compactionControlSource.clone()
                    .segment("_compcontrol", "stash-time", id)
                    .queryParam("dataCenter", dataCenter)
                    .build();
            _client.resource(uri)
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                    .delete();
        } catch (UniformInterfaceException e) {
            throw convertException(e);
        }
    }

    @Override
    public StashRunTimeInfo getStashTime(String id, String dataCenter) {
        checkNotNull(id, "id");
        checkNotNull(dataCenter, "dataCenter");

        try {
            URI uri = _compactionControlSource.clone()
                    .segment("_compcontrol", "stash-time", id)
                    .queryParam("dataCenter", dataCenter)
                    .build();
            return _client.resource(uri)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                    .get(StashRunTimeInfo.class);
        } catch (UniformInterfaceException e) {
            throw convertException(e);
        }
    }

    @Override
    public Map<StashTimeKey, StashRunTimeInfo> getAllStashTimes() {
        try {
            URI uri = _compactionControlSource.clone()
                    .segment("_compcontrol", "stash-time")
                    .build();
            return _client.resource(uri)
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                    .get(Map.class);
        } catch (UniformInterfaceException e) {
            throw convertException(e);
        }
    }

    @Override
    public Map<StashTimeKey, StashRunTimeInfo> getStashTimesForPlacement(String placement) {
        checkNotNull(placement, "placement");

        try {
            URI uri = _compactionControlSource.clone()
                    .segment("_compcontrol", "stash-time")
                    .queryParam("placement", placement)
                    .build();
            return _client.resource(uri)
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                    .get(Map.class);
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
}