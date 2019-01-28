package com.bazaarvoice.emodb.common.jersey.dropwizard;

import com.bazaarvoice.emodb.client.EmoClientException;
import com.bazaarvoice.emodb.client.EmoResource;
import com.bazaarvoice.emodb.client.EmoResponse;
import com.bazaarvoice.emodb.client.EntityHelper;
import com.fasterxml.jackson.core.type.TypeReference;

import javax.annotation.Nullable;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;

import static java.util.Objects.requireNonNull;

/**
 * EmoResource implementation that uses a WebResource returned by a Jersey client.
 */
public class JerseyEmoResource implements EmoResource {

    private WebTarget _target;
    private Invocation.Builder _builder;
    private MediaType _type;

    JerseyEmoResource(WebTarget resource) {
        _target = requireNonNull(resource, "target");
    }

    @Override
    public EmoResource queryParam(String key, String value) {
        if(_target == null) {
            throw new IllegalStateException("Invalid state to add a query param");
        };
        _target = _target.queryParam(key, value);
        return this;
    }

    @Override
    public EmoResource path(String path) {
        if (_target == null) {
            throw new IllegalStateException("Invalid state to add a path");
        }
        _target = _target.path(path);
        return this;
    }

    @Override
    public EmoResource header(String header, Object value) {
        _builder = builder().header(header, value);
        return this;
    }

    @Override
    public EmoResource type(MediaType mediaType) {
        _type = mediaType;
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
            Response response;
            if (entity == null) {
                response = builder().method(method);
            } else {
                response = builder().method(method, Entity.entity(entity, type()));
            }

            // This is as per jax-rs invocation builder code.
            if (!response.getStatusInfo().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                throw new WebApplicationException(response);
            }

            return null;
        } catch (WebApplicationException e) {
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
                    builder().method(method) :
                    builder().method(method, Entity.entity(entity, type())));
        }

        try {
            Response response = entity == null ?
                    builder().method(method) :
                    builder().method(method, Entity.entity(entity, type()));

            // This is as per jax-rs invocation builder code.
            if (!response.getStatusInfo().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                throw new WebApplicationException(response);
            }

            if (!response.getMediaType().equals(MediaType.APPLICATION_JSON_TYPE)) {
                return response.readEntity(responseType);
            }
            return EntityHelper.getEntity(response.readEntity(InputStream.class), responseType);
        } catch (WebApplicationException e) {
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
                    builder().method(method, Entity.entity(entity, type()), InputStream.class);

            return EntityHelper.getEntity(responseStream, responseType);
        } catch (WebApplicationException e) {
            throw asEmoClientException(e);
        }
    }

    /** Returns a thin EmoResponse wrapper around the Jersey response. */
    private EmoResponse toEmoResponse(Response response) {
        return new JerseyEmoResponse(response);
    }

    /** Returns an EmoClientException with a thin wrapper around the Jersey exception response. */
    private EmoClientException asEmoClientException(WebApplicationException e)
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

    private Invocation.Builder builder() {
        if (_builder == null) {
            _builder = _target.request();
            // Null resource out to force no further resource-only calls, such a queryParam().
            _target = null;
        }
        return _builder;
    }

    private MediaType type() {
        requireNonNull(_type, "You must set the media type for the request");
        return _type;
    }
}