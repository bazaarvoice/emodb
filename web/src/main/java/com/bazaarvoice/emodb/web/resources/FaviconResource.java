package com.bazaarvoice.emodb.web.resources;

import com.google.common.base.Splitter;
import com.google.common.hash.Hashing;
import com.google.common.net.HttpHeaders;
import com.sun.jersey.spi.resource.Singleton;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.Response;
import java.util.Date;

@Singleton
@Path("/favicon.ico")
public class FaviconResource {
    /** The Bazaarvoice favicon data. */
    private static final byte[] FAVICON = new byte[] {
            0, 0, 1, 0, 1, 0, 16, 16, 0, 0, 1, 0, 24, 0, 104, 3, 0, 0, 22, 0, 0, 0, 40, 0, 0, 0, 16, 0, 0, 0, 32, 0, 0,
            0, 1, 0, 24, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 103, 103, 110, 0, 0, 113, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 127, 69, 69, 100,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 60, 60, 60, -13, -13, -15, 0, 0, -96, 0, 0, 121, 0, 0,
            -126, 0, 0, -112, 0, 0, 127, 0, 0, -81, 0, 0, -75, -106, -106, -74, -128, -128, -128, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, -114, -114, -114, -6, -6, -15, 0, 0, -97, 0, 0, -102, 0, 0, 111, 0, 0, 96, 0, 0, -103, 0,
            0, -112, 0, 0, -94, -109, -109, -83, -73, -73, -73, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 23, 23, 15,
            -123, -123, -80, 0, 0, -100, 0, 0, -87, 0, 0, -84, 0, 0, -84, 0, 0, -86, 0, 0, -85, 0, 0, 89, 68, 68, -95, 42,
            42, 48, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -114, 0, 0, 37, 0, 0, -77, 0, 0, -83, 0, 0,
            -86, 0, 0, -85, 0, 0, -77, 0, 0, 25, 0, 0, -110, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 89, 0, 0, 8, 19, 19, -125, -121, -121, -71, -85, -85, -56, -94, -94, -61, 105, 105, -83, 0, 0, 53, 0, 0,
            1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 8, 7, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -76, -76, -76, -81, -81, -84, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, -104, -104, -104, -94, -94, -94, 47, 47, 47, -31, -31, -31, -1, -1, -1, 0, 0, 0, -61, -61, -61,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 4, 4, -1, -1, -1, 20, 20, 20, 25, 25,
            25, 101, 101, 101, -106, -106, -106, 0, 0, 0, 45, 45, 45, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 11, 11, 11, -1, -1, -1, 104, 104, 104, -1, -1, -1, -7, -7, -7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -71, -71, -71, 18, 18, 18, -28, -28,
            -28, 17, 17, 17, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, -13, -25, 0, 0, -16, 3, 0, 0, -16, 3, 0, 0, -16, 7, 0, 0, -16, 7, 0, 0, -8, 7, 0, 0, -8, 7, 0, 0, -16, 3,
            0, 0, -32, 3, 0, 0, -32, 3, 0, 0, -32, 3, 0, 0, -32, 3, 0, 0, -16, 3, 0, 0, -16, 3, 0, 0, -8, 7, 0, 0, -4,
            31, 0, 0};
    private static final String CONTENT_TYPE = "image/x-icon";
    private static final Date LAST_MODIFIED = new Date();
    private static final EntityTag ETAG = new EntityTag(Hashing.murmur3_128().hashBytes(FAVICON).toString());
    private static final Response NOT_MODIFIED = Response.status(HttpServletResponse.SC_NOT_MODIFIED).tag(ETAG).build();
    private static final Splitter ETAG_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

    @GET
    public Response get(@HeaderParam(HttpHeaders.IF_NONE_MATCH) String ifNoneMatch,
                        @HeaderParam(HttpHeaders.IF_MODIFIED_SINCE) Date ifModifiedSince) {
        // Check the ETags to see if the resource has changed...
        if (ifNoneMatch != null) {
            for (String eTag : ETAG_SPLITTER.split(ifNoneMatch)) {
                if ("*".equals(eTag) || ETAG.equals(EntityTag.valueOf(eTag))) {
                    return NOT_MODIFIED;
                }
            }
        }

        // Check the last modification time
        if (ifModifiedSince != null && ifModifiedSince.after(LAST_MODIFIED)) {
            return NOT_MODIFIED;
        }

        return Response.ok().lastModified(LAST_MODIFIED).tag(ETAG).type(CONTENT_TYPE).entity(FAVICON).build();
    }
}
