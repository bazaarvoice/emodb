package com.bazaarvoice.emodb.client2;

import com.fasterxml.jackson.core.type.TypeReference;

import java.io.InputStream;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Response class that can be returned as the entity for a API requests.
 */
public interface EmoResponse {

    /** Returns the HTTP status of the request, such as 200. */
    int getStatus();

    /** Returns the headers for a header key. */
    List<String> getHeader(String header);

    /** Returns the first header for a header key, or null if the header had no values. */
    String getFirstHeader(String header);

    /** Returns an iterable of all header keys and values. */
    Iterable<Map.Entry<String, List<String>>> getHeaders();

    /** Returns true if the response contains an entity. */
    boolean hasEntity();

    /** Returns an input stream with the raw entity contents. */
    InputStream getEntityInputStream();

    /** Returns the response entity transformed to the given type. */
    <T> T getEntity(Class<T> clazz);

    /** Returns the response entity transformed to the given type reference. */
    <T> T getEntity(TypeReference<T> type);

    /** Returns the response "Location" header content. */
    URI getLocation();

    /** Returns the reponse "Last-Modified" header content. */
    Date getLastModified();
}
