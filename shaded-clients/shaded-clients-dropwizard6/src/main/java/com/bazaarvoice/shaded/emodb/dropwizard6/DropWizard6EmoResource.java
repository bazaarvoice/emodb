package com.bazaarvoice.shaded.emodb.dropwizard6;

import com.bazaarvoice.emodb.client.EmoClientException;
import com.bazaarvoice.emodb.client.EmoResource;
import com.bazaarvoice.emodb.client.EmoResponse;
import com.bazaarvoice.emodb.client.EntityHelper;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import shaded.emodb.com.fasterxml.jackson.core.type.TypeReference;

import javax.annotation.Nullable;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * EmoResource implementation that uses a WebResource returned by a Jersey client with a version compatible
 * with Dropwizard 6.
 */
public class DropWizard6EmoResource implements EmoResource {

    private WebResource _resource;
    private WebResource.Builder _builder;
    private MediaType _mediaType;

    DropWizard6EmoResource(WebResource resource) {
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
        _builder = builder().type(mediaType.toString());
        // Save the media type for property entity handling later
        _mediaType = mediaType;
        return this;
    }

    @Override
    public EmoResource accept(MediaType mediaType) {
        _builder = builder().accept(mediaType.toString());
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
                builder().method(method, shadingSafeEntity(entity));
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
                    builder().method(method, ClientResponse.class, shadingSafeEntity(entity)));
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
                    builder().method(method, InputStream.class, shadingSafeEntity(entity));

            return EntityHelper.getEntity(responseStream, responseType);
        } catch (UniformInterfaceException e) {
            throw asEmoClientException(e);
        }
    }

    /**
     * Due to shading the original Jackson annotations (com.fasterxml.jackson.annotation.*) have all been relocated.
     * Any unshaded ObjectMapper will therefore not recognize these annotations and won't serialize or deserialize
     * properly.  To avoid this issue all entities must be seralized and deserialized using the *shaded* ObjectMapper.
     * Responses are already covered by {@link EntityHelper#getEntity(java.io.InputStream, Class)}.  This object
     * wrapping ensures that requests are similarly serialized using a shaded ObjectMapper instance.
     */
    private Object shadingSafeEntity(final Object entity) {
        if (MediaType.APPLICATION_JSON_TYPE.equals(_mediaType) && !(entity instanceof StreamingOutput)) {
            return new StreamingOutput() {
                @Override
                public void write(OutputStream output) throws IOException, WebApplicationException {
                    JsonHelper.writeJson(output, entity);
                }
            };
        }
        return entity;
    }

    /** Returns a thin EmoResponse wrapper around the Jersey response. */
    private EmoResponse toEmoResponse(ClientResponse clientResponse) {
        return new DropWizard6EmoResponse(clientResponse);
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

