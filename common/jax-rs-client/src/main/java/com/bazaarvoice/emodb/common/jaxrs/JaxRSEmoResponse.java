package com.bazaarvoice.emodb.common.jaxrs;

import com.bazaarvoice.emodb.client.EmoResponse;
import com.bazaarvoice.emodb.client.EntityHelper;
import com.fasterxml.jackson.core.type.TypeReference;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
// * EmoResponse implementation that wraps a jax-rs {@link Response}.
 */
public class JaxRSEmoResponse implements EmoResponse {

    private final Response _response;

    public JaxRSEmoResponse(Response response) {
        _response = checkNotNull(response, "response");
    }

    @Override
    public int getStatus() {
        return _response.getStatus();
    }

    @Override
    public List<String> getHeader(String header) {
        return _response.getStringHeaders().get(header);
    }

    @Override
    public String getFirstHeader(String header) {
        return _response.getStringHeaders().getFirst(header);
    }

    @Override
    public Iterable<Map.Entry<String, List<String>>> getHeaders() {
        return _response.getStringHeaders().entrySet();
    }

    @Override
    public boolean hasEntity() {
        return _response.hasEntity();
    }

    @Override
    public InputStream getEntityInputStream() {
        return _response.readEntity(InputStream.class);
    }

    @Override
    public <T> T getEntity(Class<T> clazz) {
        // Don't attempt to read the entity as JSON unless the header indicates to do so.
        if (MediaType.APPLICATION_JSON_TYPE.equals(_response.getMediaType())) {
            return EntityHelper.getEntity(getEntityInputStream(), clazz);
        }
        return _response.readEntity(clazz);
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
