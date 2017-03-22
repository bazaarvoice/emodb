package com.bazaarvoice.emodb.web.resources.uac;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.uac.api.UserAccessControlRequest;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;

import javax.ws.rs.Consumes;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

/**
 * Jersey reader for user access control request objects.
 */
@Provider
@Consumes({
        "application/x.json-create-role",
        "application/x.json-update-role",
        "application/x.json-create-api-key",
        "application/x.json-update-api-key"
})
public class UserAccessControlRequestMessageBodyReader implements MessageBodyReader<UserAccessControlRequest> {

    @Override
    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return true;
    }

    @Override
    public UserAccessControlRequest readFrom(Class<UserAccessControlRequest> type, Type genericType, Annotation[] annotations,
                      MediaType mediaType, MultivaluedMap<String, String> httpHeaders,
                      InputStream entityStream)
            throws IOException, WebApplicationException {
        String json = CharStreams.toString(new InputStreamReader(entityStream, Charsets.UTF_8));
        return JsonHelper.fromJson(json, type);
    }
}
