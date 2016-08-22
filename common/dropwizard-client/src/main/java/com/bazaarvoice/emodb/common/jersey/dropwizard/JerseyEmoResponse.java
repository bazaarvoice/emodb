package com.bazaarvoice.emodb.common.jersey.dropwizard;

import com.bazaarvoice.emodb.client.EmoResponse;
import com.bazaarvoice.emodb.client.EntityHelper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.sun.jersey.api.client.ClientResponse;

import javax.ws.rs.core.MediaType;
import java.io.InputStream;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * EmoResponse implementation that wraps a Jersey {@link ClientResponse}.
 */
public class JerseyEmoResponse implements EmoResponse {

    private final ClientResponse _response;

    public JerseyEmoResponse(ClientResponse response) {
        _response = checkNotNull(response, "response");
    }

    @Override
    public int getStatus() {
        return _response.getStatus();
    }

    @Override
    public List<String> getHeader(String header) {
        return _response.getHeaders().get(header);
    }

    @Override
    public String getFirstHeader(String header) {
        return _response.getHeaders().getFirst(header);
    }

    @Override
    public Iterable<Map.Entry<String, List<String>>> getHeaders() {
        return _response.getHeaders().entrySet();
    }

    @Override
    public boolean hasEntity() {
        return _response.hasEntity();
    }

    @Override
    public InputStream getEntityInputStream() {
        return _response.getEntityInputStream();
    }

    @Override
    public <T> T getEntity(Class<T> clazz) {
        // Don't attempt to read the entity as JSON unless the header indicates to do so.
        if (MediaType.APPLICATION_JSON_TYPE.equals(_response.getType())) {
            return EntityHelper.getEntity(getEntityInputStream(), clazz);
        }
        return _response.getEntity(clazz);
    }

    @Override
    public <T> T getEntity(TypeReference<T> type) {
        return EntityHelper.getEntity(getEntityInputStream(), type);
    }

    @Override
    public URI getLocation() {
        return _response.getLocation();
    }

    @Override
    public Date getLastModified() {
        return _response.getLastModified();
    }
}
