package com.bazaarvoice.emodb.client;

import com.bazaarvoice.emodb.client.EmoResponse;
import com.bazaarvoice.emodb.client.EntityHelper;
import com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;


/**
 * EmoResponse implementation that wraps a Jersey {@link Response}.
 */
public class Jersey2EmoResponse implements EmoResponse {

    private final Response _response;

    private final Logger _log = LoggerFactory.getLogger(Jersey2EmoResponse.class);
    public Jersey2EmoResponse(Response response) {
        _response = requireNonNull(response, "response");
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
        _log.info("test 10--> "+_response.hasEntity());
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
