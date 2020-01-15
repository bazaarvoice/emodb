package com.bazaarvoice.megabus.resource;

import com.bazaarvoice.emodb.common.json.JsonStreamingArrayParser;
import com.bazaarvoice.megabus.MegabusRef;
import com.bazaarvoice.megabus.MegabusSource;
import com.google.common.base.Strings;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.InputStream;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Path ("/megabus/1")
@Produces (MediaType.APPLICATION_JSON)
public class MegabusResource1 {

    private final MegabusSource _megabusSource;

    public MegabusResource1(MegabusSource megabusSource) {
        _megabusSource = checkNotNull(megabusSource, "megabusSource");
    }

    /**
     * Touch a document deliberately to send it to Megabus.
     */
    @POST
    @Path ("{table}/{key}")
    public SuccessResponse touch(@PathParam ("table") String table,
                                 @PathParam ("key") String key) {
        checkArgument(!Strings.isNullOrEmpty(table), "table is required");
        checkArgument(!Strings.isNullOrEmpty(key), "key is required");

        _megabusSource.touch(table, key);

        return SuccessResponse.instance();
    }

    /**
     * Touch multiple documents deliberately to send them to Megabus.
     * Imports an arbitrary size stream of an array of JSON {@link MegabusRef} objects.
     */
    @POST
    @Path ("_stream")
    @Consumes (MediaType.APPLICATION_JSON)
    public SuccessResponse touchAll(InputStream in) {

        Iterator<MegabusRef> allRefs = new JsonStreamingArrayParser<>(in, MegabusRef.class);

        _megabusSource.touchAll(allRefs);

        return SuccessResponse.instance();
    }

}