package com.bazaarvoice.emodb.web.megabus.resource;

import com.bazaarvoice.emodb.common.json.JsonStreamingArrayParser;
import com.bazaarvoice.emodb.sor.api.Coordinate;
import com.bazaarvoice.emodb.web.resources.SuccessResponse;
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
import static java.util.Objects.requireNonNull;

@Path ("/megabus/1")
@Produces (MediaType.APPLICATION_JSON)
public class MegabusResource1 {

    private final MegabusSource _megabusSource;

    public MegabusResource1(MegabusSource megabusSource) {
        _megabusSource = requireNonNull(megabusSource, "megabusSource");
    }

    /**
     * Touch a document deliberately to send it to Megabus.
     */
    @POST
    @Path ("_touch/{table}/{id}")
    public SuccessResponse touch(@PathParam ("table") String table,
                                 @PathParam ("id") String id) {
        checkArgument(!Strings.isNullOrEmpty(table), "table is required");
        checkArgument(!Strings.isNullOrEmpty(id), "id is required");

        Coordinate coordinate = Coordinate.of(table, id);

        _megabusSource.touch(coordinate);

        return SuccessResponse.instance();
    }

    /**
     * Touch multiple documents deliberately to send them to Megabus.
     * Imports an arbitrary size stream of an array of JSON {@link Coordinate} objects.
     * Note that megabus ultimately gets the latest version of the available in the datacenter.
     * If the document doesn't exist, this action will result in a NULL in the megabus kafka topic for each doc.
     */
    @POST
    @Path ("_touch/_stream")
    @Consumes (MediaType.APPLICATION_JSON)
    public SuccessResponse touchAll(InputStream in) {

        Iterator<Coordinate> allCoordinates = new JsonStreamingArrayParser<>(in, Coordinate.class);

        _megabusSource.touchAll(allCoordinates);

        return SuccessResponse.instance();
    }

}
