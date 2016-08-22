package com.bazaarvoice.emodb.common.jersey.dropwizard;

import com.bazaarvoice.emodb.client.EmoClientException;
import com.bazaarvoice.emodb.client.EmoResource;
import com.bazaarvoice.emodb.client.EmoResponse;
import com.bazaarvoice.emodb.client.EntityHelper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;

import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;
import java.io.InputStream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * EmoResource implementation that uses a WebResource returned by a Jersey client.
 */
public class JerseyEmoResource implements EmoResource {

    private WebResource _resource;
    private WebResource.Builder _builder;

    JerseyEmoResource(WebResource resource) {
        _resource = checkNotNull(resource, "resource");
    }

    @Override
    public EmoResource queryParam(String key, String value) {
        checkState(_resource != null, "Invalid state to add a query param");
        _resource = _resource.queryParam(key, value);
        return this;
    }

    @Override
    public EmoResource path(String path) {
        checkState(_resource != null, "Invalid state to add a path");
        _resource = _resource.path(path);
        return this;
    }

    @Override
    public EmoResource header(String header, Object value) {
        _builder = builder().header(header, value);
        return this;
    }

    @Override
    public EmoResource type(MediaType mediaType) {
        _builder = builder().type(mediaType);
        return this;
    }

    @Override
    public EmoResource accept(MediaType mediaType) {
        _builder = builder().accept(mediaType);
        return this;
    }

    @Override
    public EmoResponse head() {
        return toEmoResponse(builder().head());
    }

    /**
     * Sends the optionally provided request entity using the provided method.
     */
    private <T> T send(String method, @Nullable Object entity) {
        try {
            if (entity == null) {
                builder().method(method);
            } else {
                builder().method(method, entity);
            }
            return null;
        } catch (UniformInterfaceException e) {
            throw asEmoClientException(e);
        }
    }

    /**
     * Sends the optionally provided request entity using the provided method.  The response entity is then
     * deserialized to the provided type.
     */
    private <T> T send(String method, Class<T> responseType, @Nullable Object entity) {
        // Narrow the response type
        if (responseType == EmoResponse.class) {
            // noinspection unchecked
            return (T) toEmoResponse(entity == null ?
                    builder().method(method, ClientResponse.class) :
                    builder().method(method, ClientResponse.class, entity));
        }

        try {
            ClientResponse response = entity == null ?
                    builder().method(method, ClientResponse.class) :
                    builder().method(method, ClientResponse.class, entity);

            // This is as per Jersey's WebResource builder() code.
            if (response.getStatus() >= 300) {
                throw new UniformInterfaceException(response);
            }

            if (!response.getType().equals(MediaType.APPLICATION_JSON_TYPE)) {
                return response.getEntity(responseType);
            }
            return EntityHelper.getEntity(response.getEntity(InputStream.class), responseType);
        } catch (UniformInterfaceException e) {
            throw asEmoClientException(e);
        }
    }

    /**
     * Sends the optionally provided request entity using the provided method.  The response entity is then
     * deserialized to the provided type reference.
     */
    private <T> T send(String method, TypeReference<T> responseType, @Nullable Object entity) {
        try {
            InputStream responseStream = entity == null ?
                    builder().method(method, InputStream.class) :
                    builder().method(method, InputStream.class, entity);

            return EntityHelper.getEntity(responseStream, responseType);
        } catch (UniformInterfaceException e) {
            throw asEmoClientException(e);
        }
    }

    /** Returns a thin EmoResponse wrapper around the Jersey response. */
    private EmoResponse toEmoResponse(ClientResponse clientResponse) {
        return new JerseyEmoResponse(clientResponse);
    }

    /** Returns an EmoClientException with a thin wrapper around the Jersey exception response. */
    private EmoClientException asEmoClientException(UniformInterfaceException e)
            throws EmoClientException {
        throw new EmoClientException(e.getMessage(), e, toEmoResponse(e.getResponse()));
    }

    @Override
    public <T> T get(Class<T> clazz) {
        return send("GET", clazz, null);
    }

    @Override
    public <T> T get(TypeReference<T> type) {
        return send("GET", type, null);
    }

    @Override
    public void post() {
        send("POST", null);
    }

    @Override
    public void post(Object entity) {
        send("POST", entity);
    }

    @Override
    public <T> T post(Class<T> clazz, Object entity) {
        return send("POST", clazz, entity);
    }

    @Override
    public <T> T post(TypeReference<T> type, Object entity) {
        return send("POST", type, entity);
    }

    @Override
    public void put() {
        send("PUT", null);
    }

    @Override
    public void put(Object entity) {
        send("PUT", entity);
    }

    @Override
    public <T> T put(Class<T> clazz, Object entity) {
        return send("PUT", clazz, entity);
    }

    @Override
    public <T> T put(TypeReference<T> type, Object entity) {
        return send("PUT", type, entity);
    }

    @Override
    public void delete() {
        send("DELETE", null);
    }

    @Override
    public <T> T delete(Class<T> clazz) {
        return send("DELETE", clazz, null);
    }

    @Override
    public <T> T delete(TypeReference<T> type) {
        return send("DELETE", type, null);
    }

    private WebResource.Builder builder() {
        if (_builder == null) {
            _builder = _resource.getRequestBuilder();
            // Null resource out to force no further resource-only calls, such a queryParam().
            _resource = null;
        }
        return _builder;
    }
}
