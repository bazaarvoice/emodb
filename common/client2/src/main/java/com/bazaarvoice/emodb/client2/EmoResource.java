package com.bazaarvoice.emodb.client2;

import com.fasterxml.jackson.core.type.TypeReference;

import javax.ws.rs.core.MediaType;

/**
 * Defines the fluent interface for making API resource requests to the EmoDB service.
 */
public interface EmoResource2 {

    // The following two methods -- queryParam() and path() -- must be called prior to any other calls in this interface.

    /** Adds a query parameter to the request. */
    EmoResource2 queryParam(String key, String value);

    /** Appends the path element to the request path */
    EmoResource2 path(String path);

    /** Adds a header to the request. */
    EmoResource2 header(String header, Object value);

    /** Sets the media type for the request. */
    EmoResource2 type(MediaType mediaType);

    /** Adds an accepted response media type to the request. */
    EmoResource2 accept(MediaType mediaType);

    /** Performs a HEAD request. */
    EmoResponse2 head();

    /** Performs a GET request and transforms the response entity to the given type. */
    <T> T get(Class<T> clazz);

    /** Performs a GET request and transforms the response entity to the given type reference. */
    <T> T get(TypeReference<T> type);

    /** Performs a POST request. */
    void post();

    /** Performs a POST request with the provided request entity. */
    void post(Object entity);

    /** Performs a POST request with the provided request entity and transforms the response entity to the given type. */
    <T> T post(Class<T> clazz, Object entity);

    /** Performs a POST request with the provided request entity and transforms the response entity to the given type reference. */
    <T> T post(TypeReference<T> type, Object entity);

    /** Performs a PUT request. */
    void put();

    /** Performs a PUT request with the provided request entity. */
    void put(Object entity);

    /** Performs a PUT request with the provided request entity and transforms the response entity to the given type. */
    <T> T put(Class<T> clazz, Object entity);

    /** Performs a PUT request with the provided request entity and transforms the response entity to the given type reference. */
    <T> T put(TypeReference<T> type, Object entity);

    /** Performs a DELETE request. */
    void delete();

    /** Performs a DELETE request and transforms the response entity to the given type. */
    <T> T delete(Class<T> clazz);

    /** Performs a DELETE request and transforms the response entity to the given type reference. */
    <T> T delete(TypeReference<T> type);
}
