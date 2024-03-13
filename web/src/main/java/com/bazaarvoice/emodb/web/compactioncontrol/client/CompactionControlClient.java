package com.bazaarvoice.emodb.web.compactioncontrol.client;

import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.api.StashRunTimeInfo;
import com.bazaarvoice.emodb.sor.api.StashTimeKey;
import org.glassfish.jersey.client.ClientResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.WebApplicationException;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class CompactionControlClient implements CompactionControlSource {

    private final EmoClient _client;
    private final UriBuilder _compactionControlSource;
    private final String _apiKey;

    public CompactionControlClient(URI endPoint, EmoClient jerseyClient, String apiKey) {
        _client = requireNonNull(jerseyClient, "jerseyClient");
        _compactionControlSource = UriBuilder.fromUri(endPoint);
        _apiKey = requireNonNull(apiKey, "apiKey");
    }

    @Override
    public void updateStashTime(String id, long timestamp, List<String> placements, long expiredTimestamp, String dataCenter) {
        requireNonNull(id, "id");
        requireNonNull(placements, "placements");
        requireNonNull(dataCenter, "dataCenter");

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
        } catch (Exception e) {
           // throw convertException(e);
            throw new RuntimeException("Failed in updateStashTime: ", e);
        }
    }

    @Override
    public void deleteStashTime(String id, String dataCenter) {
        requireNonNull(id, "id");
        requireNonNull(dataCenter, "dataCenter");

        try {
            URI uri = _compactionControlSource.clone()
                    .segment("_compcontrol", "stash-time", id)
                    .queryParam("dataCenter", dataCenter)
                    .build();
            _client.resource(uri)
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                    .delete();
        } catch (Exception e) {
            //throw convertException(e);
            throw new RuntimeException("Failed in deleteStashTime: ", e);
        }
    }

    @Override
    public StashRunTimeInfo getStashTime(String id, String dataCenter) {
        requireNonNull(id, "id");
        requireNonNull(dataCenter, "dataCenter");

        try {
            URI uri = _compactionControlSource.clone()
                    .segment("_compcontrol", "stash-time", id)
                    .queryParam("dataCenter", dataCenter)
                    .build();
            return _client.resource(uri)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                    .get(StashRunTimeInfo.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed in getStashTime: ", e);
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
        } catch (Exception e) {
            //throw convertException(e);
            throw new RuntimeException("Failed in getAllStashTimes: ", e);
        }
    }

    @Override
    public Map<StashTimeKey, StashRunTimeInfo> getStashTimesForPlacement(String placement) {
        requireNonNull(placement, "placement");

        try {
            URI uri = _compactionControlSource.clone()
                    .segment("_compcontrol", "stash-time")
                    .queryParam("placement", placement)
                    .build();
            return _client.resource(uri)
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                    .get(Map.class);
        } catch (Exception e) {
            //throw convertException(e);
            throw new RuntimeException("Failed in getStashTimesForPlacement: ", e);
        }
    }

    /*private RuntimeException convertException(WebApplicationException e) {
        Response response = e.getResponse();
        String exceptionType = response.getFirstHeader("X-BV-Exception");

        if (response.getStatus() == Response.Status.BAD_REQUEST.getStatusCode() &&
                IllegalArgumentException.class.getName().equals(exceptionType)) {
            return new IllegalArgumentException(response.readEntity(String.class), e);
        }
        return e;
    }*/
}