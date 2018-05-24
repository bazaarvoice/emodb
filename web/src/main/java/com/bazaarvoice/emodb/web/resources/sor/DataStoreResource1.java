package com.bazaarvoice.emodb.web.resources.sor;

import com.bazaarvoice.emodb.auth.jersey.Authenticated;
import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.common.api.UnauthorizedException;
import com.bazaarvoice.emodb.common.dropwizard.jersey.Unbuffered;
import com.bazaarvoice.emodb.common.json.JsonStreamingArrayParser;
import com.bazaarvoice.emodb.common.json.LoggingIterator;
import com.bazaarvoice.emodb.common.json.OrderedJson;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.api.Coordinate;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.FacadeOptions;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.PurgeStatus;
import com.bazaarvoice.emodb.sor.api.Table;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.UnpublishedDatabusEvent;
import com.bazaarvoice.emodb.sor.api.Update;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.core.DataStoreAsync;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.delta.Literal;
import com.bazaarvoice.emodb.sor.delta.MapDelta;
import com.bazaarvoice.emodb.web.auth.Permissions;
import com.bazaarvoice.emodb.web.auth.resource.CreateTableResource;
import com.bazaarvoice.emodb.web.auth.resource.NamedResource;
import com.bazaarvoice.emodb.web.jersey.FilteredJsonStreamingOutput;
import com.bazaarvoice.emodb.web.jersey.params.InstantParam;
import com.bazaarvoice.emodb.web.jersey.params.SecondsParam;
import com.bazaarvoice.emodb.web.jersey.params.TimeUUIDParam;
import com.bazaarvoice.emodb.web.resources.SuccessResponse;
import com.bazaarvoice.emodb.web.resources.compactioncontrol.CompactionControlResource1;
import com.bazaarvoice.emodb.web.throttling.ThrottleConcurrentRequests;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import io.dropwizard.jersey.params.AbstractParam;
import io.dropwizard.jersey.params.BooleanParam;
import io.dropwizard.jersey.params.IntParam;
import io.dropwizard.jersey.params.LongParam;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.shiro.authz.annotation.RequiresAuthentication;
import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.io.InputStream;
import java.io.Reader;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

@Path ("/sor/1")
@Produces (MediaType.APPLICATION_JSON)
@Api (value = "System of Record: ", description = "All DataStore operations")
@RequiresAuthentication
public class DataStoreResource1 {
    private static final Logger _log = LoggerFactory.getLogger(DataStoreResource1.class);

    /**
     * To distinguish between UUID and timestamps, assume anything that looks like "hex-hex-hex-hex-hex" is a UUID.
     */
    private static final Pattern UUID_LIKE_PATTERN = Pattern.compile("[0-9a-fA-F]+(-[0-9a-fA-F]+){4}");

    private final DataStore _dataStore;
    private final DataStoreAsync _dataStoreAsync;
    private final CompactionControlSource _compactionControlSource;

    public DataStoreResource1(DataStore dataStore, DataStoreAsync dataStoreAsync, CompactionControlSource compactionControlSource) {
        _dataStore = dataStore;
        _dataStoreAsync = dataStoreAsync;
        _compactionControlSource = compactionControlSource;
    }

    @Path ("_compcontrol")
    public CompactionControlResource1 getCompactionControlResource() {
        return new CompactionControlResource1(_compactionControlSource);
    }

    @GET
    @Path ("_table")
    @Timed (name = "bv.emodb.sor.DataStoreResource1.listTables", absolute = true)
    @Unbuffered
    @ApiOperation (value = "Returns all the existing tables",
            notes = "Returns a Iterator of Table",
            response = Table.class
    )
    public Object listTables(final @QueryParam ("from") String fromKeyExclusive,
                             final @QueryParam ("limit") @DefaultValue ("10") LongParam limitParam,
                             final @Authenticated Subject subject) {
        Iterator<Table> allTables = _dataStore.listTables(Strings.emptyToNull(fromKeyExclusive), Long.MAX_VALUE);
        return new FilteredJsonStreamingOutput<Table>(allTables, limitParam.get()) {
            @Override
            public boolean include(Table table) {
                return subject.hasPermission(Permissions.readSorTable(new NamedResource(table.getName())));
            }
        };
    }

    @GET
    @Path ("_unpublishedevents")
    @Timed (name = "bv.emodb.sor.DataStoreResource1.listUnpublishedDatabusEvents", absolute = true)
    @Unbuffered
    @ApiOperation (value = "Returns all the emo table events that are not published on the databus.",
            notes = "Returns a Iterator of a Map.",
            response = Table.class
    )
    public Object listUnpublishedDatabusEvents(final @QueryParam ("from") InstantParam fromInclusiveParam,
                                            final @QueryParam ("to") InstantParam toExclusiveParam,
                                            final @Authenticated Subject subject) {
        // Default date range is past 30 days
        Instant fromInclusive = fromInclusiveParam != null ? fromInclusiveParam.get() : Instant.now().minus(Duration.ofDays(29));
        Instant toExclusive = toExclusiveParam != null ? toExclusiveParam.get() : Instant.now().plus(Duration.ofDays(1));
        checkArgument(fromInclusive.compareTo(toExclusive) < 0, "from date must be before the to date.");

        Iterator<UnpublishedDatabusEvent> allUnpublishedDatabusEvents =
                _dataStore.listUnpublishedDatabusEvents(Date.from(fromInclusive), Date.from(toExclusive));
        return new FilteredJsonStreamingOutput<UnpublishedDatabusEvent>(allUnpublishedDatabusEvents, Long.MAX_VALUE) {
            @Override
            public boolean include(UnpublishedDatabusEvent event) {
                return subject.hasPermission(Permissions.readSorTable(new NamedResource(event.getTable())));
            }
        };
    }

    @PUT
    @Path ("_table/{table}")
    @Consumes (MediaType.APPLICATION_JSON)
    @Timed (name = "bv.emodb.sor.DataStoreResource1.createTable", absolute = true)
    @ApiOperation (value = "Creates a table",
            notes = "Returns a SuccessResponse if table is created",
            response = SuccessResponse.class
    )
    public SuccessResponse createTable(@PathParam ("table") String table,
                                       @QueryParam ("options") TableOptionsParam optionParams,
                                       Map<String, Object> template,
                                       @QueryParam ("audit") AuditParam auditParam,
                                       @Context UriInfo uriInfo,
                                       @Authenticated Subject subject) {
        TableOptions options = getRequired(optionParams, "options");
        Audit audit = getRequired(auditParam, "audit");

        // Check permission for creating this table
        CreateTableResource resource = new CreateTableResource(table, options.getPlacement(), template);
        if (!subject.hasPermission(Permissions.createSorTable(resource))) {
            throw new UnauthorizedException();
        }

        _dataStore.createTable(table, options, template, audit);
        return SuccessResponse.instance();
    }

    @DELETE
    @Path ("_table/{table}")
    @RequiresPermissions ("sor|drop_table|{table}")
    @Timed (name = "bv.emodb.sor.DataStoreResource1.dropTable", absolute = true)
    @ApiOperation (value = "Drops a table",
            notes = "Returns a SuccessResponse if table is dropped",
            response = SuccessResponse.class
    )
    public SuccessResponse dropTable(@PathParam ("table") String table,
                                     @QueryParam ("audit") AuditParam auditParam,
                                     @Context UriInfo uriInfo) {
        Audit audit = getRequired(auditParam, "audit");
        _dataStore.dropTable(table, audit);
        return SuccessResponse.instance();
    }

    @PUT
    @Path ("_facade/{table}")
    @Consumes (MediaType.APPLICATION_JSON)
    @RequiresPermissions ("facade|create_facade|{table}")
    @Timed (name = "bv.emodb.sor.DataStoreResource1.createFacade", absolute = true)
    @ApiOperation (value = "Creates a facade",
            notes = "Returns a SuccessResponse if facade is created",
            response = SuccessResponse.class
    )
    public SuccessResponse createFacade(@PathParam ("table") String table,
                                        @QueryParam ("options") FacadeDefinitionParam optionParams,
                                        @QueryParam ("audit") AuditParam auditParam,
                                        @Context UriInfo uriInfo) {
        FacadeOptions options = getRequired(optionParams, "options");
        Audit audit = getRequired(auditParam, "audit");
        _dataStore.createFacade(table, options, audit);
        return SuccessResponse.instance();
    }

    @DELETE
    @Path ("_facade/{table}")
    @RequiresPermissions ("facade|drop_facade|{table}")
    @Timed (name = "bv.emodb.sor.DataStoreResource1.dropFacade", absolute = true)
    @ApiOperation (value = "Drops a Facade",
            notes = "Returns a SuccessResponse if facade is dropped",
            response = SuccessResponse.class
    )
    public SuccessResponse dropFacade(@PathParam ("table") String table,
                                      @QueryParam ("audit") AuditParam auditParam,
                                      @QueryParam ("placement") String placement,
                                      @Context UriInfo uriInfo) {
        checkArgument(!Strings.isNullOrEmpty(placement), "Missing required placement.");
        Audit audit = getRequired(auditParam, "audit");
        _dataStore.dropFacade(table, placement, audit);
        return SuccessResponse.instance();
    }


    @POST
    @Path ("_table/{table}/purge")
    @RequiresPermissions ("sor|purge|{table}")
    @Timed (name = "bv.emodb.sor.DataStoreResource1.purgeTable", absolute = true)
    @ApiOperation (value = "Purges a table",
            notes = "Returns a SuccessResponse if table is purged",
            response = SuccessResponse.class
    )
    public Map<String, Object> purgeTableAsync(@PathParam ("table") String table,
                                               @QueryParam ("audit") AuditParam auditParam) {
        Audit audit = getRequired(auditParam, "audit");
        String jobID = _dataStoreAsync.purgeTableAsync(table, audit);
        return ImmutableMap.<String, Object>of("id", jobID);
    }

    @POST
    @Path ("_table/{table}/purgestatus")
    @RequiresPermissions ("sor|purge|{table}")
    @Timed (name = "bv.emodb.sor.DataStoreResource1.purgeTable", absolute = true)
    public Map<String, Object> getPurgeStatus(@PathParam ("table") String table, @QueryParam ("id") String jobID) {
        System.out.println(jobID.toString());
        PurgeStatus purgeStatus = _dataStoreAsync.getPurgeStatus(table, jobID);

        return ImmutableMap.<String, Object>of("status", purgeStatus.getStatus());
    }

    @GET
    @Path ("_table/{table}")
    @RequiresPermissions ("sor|read|{table}")
    @Timed (name = "bv.emodb.sor.DataStoreResource1.getTableTemplate", absolute = true)
    @ApiOperation (value = "Returns a table template",
            notes = "Returns a Map",
            response = Map.class
    )
    public Map<String, Object> getTableTemplate(@PathParam ("table") String table,
                                                @QueryParam ("debug") BooleanParam debug) {
        Map<String, Object> template = _dataStore.getTableTemplate(table);
        // if debugging, sort the json result so it's easier to understand in a browser
        return optionallyOrdered(template, debug);
    }

    @PUT
    @Path ("_table/{table}/template")
    @Consumes (MediaType.APPLICATION_JSON)
    @RequiresPermissions ("sor|set_table_attributes|{table}")
    @Timed (name = "bv.emodb.sor.DataStoreResource1.setTableTemplate", absolute = true)
    @ApiOperation (value = "Sets table template",
            notes = "Returns a SuccessResponse if table template is set",
            response = SuccessResponse.class
    )
    public SuccessResponse setTableTemplate(@PathParam ("table") String table,
                                            Map<String, Object> template,
                                            @QueryParam ("audit") AuditParam auditParam,
                                            @Context UriInfo uriInfo) {
        Audit audit = getRequired(auditParam, "audit");
        _dataStore.setTableTemplate(table, template, audit);
        return SuccessResponse.instance();
    }

    @GET
    @Path ("_table/{table}/options")
    @RequiresPermissions ("sor|read|{table}")
    @Timed (name = "bv.emodb.sor.DataStoreResource1.getTableOptions", absolute = true)
    @ApiOperation (value = "Returns Table options",
            notes = "Returns a TableOptions object",
            response = TableOptions.class
    )
    public TableOptions getTableOptions(@PathParam ("table") String table) {
        return _dataStore.getTableOptions(table);
    }

    @GET
    @Path ("_table/{table}/size")
    @RequiresPermissions ("sor|read|{table}")
    @Timed (name = "bv.emodb.sor.DataStoreResource1.getTableSize", absolute = true)
    @ApiOperation (value = "Returns Table Size",
            notes = "Returns a long",
            response = long.class
    )
    public long getTableSize(@PathParam ("table") String table, @QueryParam ("limit") @Nullable IntParam limit) {
        return limit == null ?
                _dataStore.getTableApproximateSize(table) :
                _dataStore.getTableApproximateSize(table, limit.get());
    }

    @GET
    @Path ("_table/{table}/metadata")
    @RequiresPermissions ("sor|read|{table}")
    @Timed (name = "bv.emodb.sor.DataStoreResource1.getTableMetadata", absolute = true)
    @ApiOperation (value = "Returns Table metadata",
            notes = "Returns a Table object",
            response = Table.class
    )
    public Table getTableMetadata(@PathParam ("table") String table) {
        return _dataStore.getTableMetadata(table);
    }

    /**
     * Retrieves the current version of a piece of content from the data store.
     */
    @GET
    @Path ("{table}/{key}")
    @RequiresPermissions ("sor|read|{table}")
    @Timed (name = "bv.emodb.sor.DataStoreResource1.get", absolute = true)
    @ApiOperation (value = "Retrieves the current version of a piece of content from the data store.",
            notes = "Retrieves the current version of a piece of content from the data store.",
            response = Map.class
    )
    public Map<String, Object> get(@PathParam ("table") String table,
                                   @PathParam ("key") String key,
                                   @QueryParam ("consistency") @DefaultValue ("STRONG") ReadConsistencyParam consistency,
                                   @QueryParam ("debug") BooleanParam debug) {
        Map<String, Object> content = _dataStore.get(table, key, consistency.get());
        // if debugging, sort the json result so it's easier to understand in a browser
        return optionallyOrdered(content, debug);
    }

    /**
     * Retrieves all recorded history for a piece of content in the data store.
     */
    @GET
    @Path ("{table}/{key}/timeline")
    @RequiresPermissions ("sor|read|{table}")
    @Timed (name = "bv.emodb.sor.DataStoreResource1.getTimeline", absolute = true)
    @ApiOperation (value = "Retrieves all recorded history for a piece of content in the data store.",
            notes = "Retrieves all recorded history for a piece of content in the data store.",
            response = Iterator.class
    )
    public Iterator<Change> getTimeline(@PathParam ("table") String table,
                                        @PathParam ("key") String key,
                                        @QueryParam ("data") @DefaultValue ("true") BooleanParam includeContentData,
                                        @QueryParam ("audit") @DefaultValue ("false") BooleanParam includeAuditInformation,
                                        @QueryParam ("start") String startParam,
                                        @QueryParam ("end") String endParam,
                                        @QueryParam ("reversed") @DefaultValue ("true") BooleanParam reversed,
                                        @QueryParam ("limit") @DefaultValue ("10") LongParam limit,
                                        @QueryParam ("consistency") @DefaultValue ("STRONG") ReadConsistencyParam consistency) {
        // For the REST API, start & end may be either UUIDs or ISO 8601 timestamps.  If timestamps, they are inclusive
        // w/granularity of a millisecond so adjust upper endpoints to be the last valid time UUID for the millisecond.
        UUID start = parseUuidOrTimestamp(startParam, reversed.get());
        UUID end = parseUuidOrTimestamp(endParam, !reversed.get());

        return streamingIterator(_dataStore.getTimeline(table, key, includeContentData.get(),
                includeAuditInformation.get(), start, end, reversed.get(), limit.get(), consistency.get()), null);
    }

    /**
     * Retrieves a list of content items in a particular table.  To retrieve <em>all</em> items in a table set the
     * limit param to a very large value (eg. Long.MAX_VALUE), but for large tables be sure your client can stream the
     * results without exhausting all available memory.
     */
    @GET
    @Path ("{table}")
    @RequiresPermissions ("sor|read|{table}")
    @Unbuffered
    @Timed (name = "bv.emodb.sor.DataStoreResource1.scan", absolute = true)
    @ApiOperation (value = "Retrieves a list of content items in a particular table.",
            notes = "Retrieves a list of content items in a particular table.  To retrieve <em>all</em> items in a table set the\n" +
                    " limit param to a very large value (eg. Long.MAX_VALUE), but for large tables be sure your client can stream the\n" +
                    " results without exhausting all available memory.",
            response = Iterator.class
    )
    @ApiImplicitParams ({@ApiImplicitParam (name = "APIKey", required = true, dataType = "string", paramType = "query")})
    public Object scan(@PathParam ("table") String table,
                       @QueryParam ("from") String fromKeyExclusive,
                       @QueryParam ("limit") @DefaultValue ("10") LongParam limit,
                       @QueryParam ("includeDeletes") @DefaultValue ("false") BooleanParam includeDeletes,
                       @QueryParam ("consistency") @DefaultValue ("STRONG") ReadConsistencyParam consistency,
                       @QueryParam ("debug") BooleanParam debug) {
        // Always get all content, including deletes, from the backend.  That way long streams of deleted content don't
        // create long pauses in results.
        Iterator<Map<String, Object>> unfilteredContent;
        if (includeDeletes.get()) {
            unfilteredContent = _dataStore.scan(table, Strings.emptyToNull(fromKeyExclusive), limit.get(), true, consistency.get());
            return streamingIterator(unfilteredContent, debug);
        } else {
            // Can't pass limit parameter to the back-end since we may exclude deleted content.  Get all records and self-limit.
            unfilteredContent = _dataStore.scan(table, Strings.emptyToNull(fromKeyExclusive), Long.MAX_VALUE, true, consistency.get());
            return deletedContentFilteringStream(unfilteredContent, limit.get());
        }
    }

    /**
     * Retrieves a list split identifiers for the specified table.
     */
    @GET
    @Path ("_split/{table}")
    @RequiresPermissions ("sor|read|{table}")
    @Timed (name = "bv.emodb.sor.DataStoreResource1.getSplits", absolute = true)
    @ApiOperation (value = "Retrieves a list split identifiers for the specified table.",
            notes = "Retrieves a list split identifiers for the specified table.",
            response = Collection.class
    )
    public Collection<String> getSplits(@PathParam ("table") String table,
                                        @QueryParam ("size") @DefaultValue ("10000") IntParam desiredRecordsPerSplit) {
        return _dataStore.getSplits(table, desiredRecordsPerSplit.get());
    }

    /**
     * Retrieves a list of content items in a particular table split.
     */
    @GET
    @Path ("_split/{table}/{split}")
    @RequiresPermissions ("sor|read|{table}")
    @ThrottleConcurrentRequests (maxRequests = 550)
    @Unbuffered
    @Timed (name = "bv.emodb.sor.DataStoreResource1.getSplit", absolute = true)
    @ApiOperation (value = "Retrieves a list of content items in a particular table split.",
            notes = "Retrieves a list of content items in a particular table split.",
            response = Iterator.class
    )
    public Object getSplit(@PathParam ("table") String table,
                           @PathParam ("split") String split,
                           @QueryParam ("from") String key,
                           @QueryParam ("limit") @DefaultValue ("10") LongParam limit,
                           @QueryParam ("includeDeletes") @DefaultValue ("false") BooleanParam includeDeletes,
                           @QueryParam ("consistency") @DefaultValue ("STRONG") ReadConsistencyParam consistency,
                           @QueryParam ("debug") BooleanParam debug) {
        // Always get all content, including deletes, from the backend.  That way long streams of deleted content don't
        // create long pauses in results.
        Iterator<Map<String, Object>> unfilteredContent;
        if (includeDeletes.get()) {
            unfilteredContent = _dataStore.getSplit(table, split, Strings.emptyToNull(key), limit.get(), true, consistency.get());
            return streamingIterator(unfilteredContent, debug);
        } else {
            // Can't pass limit parameter to the back-end since we may exclude deleted content.  Get all records and self-limit.
            unfilteredContent = _dataStore.getSplit(table, split, Strings.emptyToNull(key), Long.MAX_VALUE, true, consistency.get());
            return deletedContentFilteringStream(unfilteredContent, limit.get());
        }
    }

    /**
     * Retrieves a list of content items for the specified comma-delimited coordinates.
     */
    @GET
    @Path ("_multiget")
    @Timed (name = "bv.emodb.sor.DataStoreResource1.multiGet", absolute = true)
    @ApiOperation (value = "Retrieves a list of content items for the specified comma-delimited coordinates.",
            notes = "Retrieves a list of content items for the specified comma-delimited coordinates.",
            response = Iterator.class
    )
    public Iterator<Map<String, Object>> multiGet(@QueryParam ("id") List<String> coordinates,
                                                  @QueryParam ("consistency") @DefaultValue ("STRONG") ReadConsistencyParam consistency,
                                                  @QueryParam ("debug") BooleanParam debug,
                                                  final @Authenticated Subject subject) {
        List<Coordinate> coordinateList = parseCoordinates(coordinates);
        for (Coordinate coordinate : coordinateList) {
            if (!subject.hasPermission(Permissions.readSorTable(new NamedResource(coordinate.getTable())))) {
                throw new UnauthorizedException("not authorized to read table " + coordinate.getTable());
            }
        }
        return streamingIterator(_dataStore.multiGet(coordinateList, consistency.get()), debug);
    }

    /**
     * Returns a list of valid table placements.
     */
    @GET
    @Path ("_tableplacement")
    @Produces (MediaType.APPLICATION_JSON)
    @Timed (name = "bv.emodb.sor.DataStoreResource1.getTablePlacements", absolute = true)
    @ApiOperation (value = "Returns a list of valid table placements.",
            notes = "Returns a list of valid table placements.",
            response = Collection.class
    )
    public Collection<String> getTablePlacements() {
        return _dataStore.getTablePlacements();
    }

    /**
     * Creates or replaces a piece of content in the data store.  Overwrites the old
     * version of the content, if it exists.  Expects a literal JSON representation
     * of the object.
     */
    @PUT
    @Path ("{table}/{key}")
    @Consumes (MediaType.APPLICATION_JSON)
    @RequiresPermissions ("sor|update|{table}")
    @Timed (name = "bv.emodb.sor.DataStoreResource1.replace", absolute = true)
    @ApiOperation (value = "Creates or replaces a piece of content in the data store.",
            notes = "Creates or replaces a piece of content in the data store.  Overwrites the old\n" +
                    " version of the content, if it exists.  Expects a literal JSON representation\n" +
                    " of the object.",
            response = SuccessResponse.class
    )
    public SuccessResponse replace(@PathParam ("table") String table,
                                   @PathParam ("key") String key,
                                   @QueryParam ("changeId") TimeUUIDParam changeIdParam,
                                   Map<String, Object> json,
                                   @QueryParam ("audit") AuditParam auditParam,
                                   @QueryParam ("consistency") @DefaultValue ("STRONG") WriteConsistencyParam consistency,
                                   @QueryParam ("tag") List<String> tags,
                                   @QueryParam ("debug") BooleanParam debug,
                                   @Authenticated Subject subject) {
        Set<String> tagsSet = (tags == null) ? ImmutableSet.<String>of() : Sets.newHashSet(tags);
        return doUpdate(table, key, changeIdParam, Deltas.literal(json), auditParam, consistency, debug, false, subject, tagsSet);
    }

    /**
     * Creates or replaces a piece of content of a facade in the data store.  Overwrites the old
     * version of the content, if it exists.  Expects a literal JSON representation
     * of the object.
     */
    @PUT
    @Path ("_facade/{table}/{key}")
    @Consumes (MediaType.APPLICATION_JSON)
    @RequiresPermissions ("facade|update|{table}")
    @Timed (name = "bv.emodb.sor.DataStoreResource1.replaceFacadeContent", absolute = true)
    @ApiOperation (value = "Creates or replaces a piece of content of a facade in the data store.",
            notes = "Creates or replaces a piece of content of a facade in the data store.  Overwrites the old\n" +
                    " version of the content, if it exists.  Expects a literal JSON representation\n" +
                    " of the object.",
            response = SuccessResponse.class
    )
    public SuccessResponse replaceFacadeContent(@PathParam ("table") String table,
                                                @PathParam ("key") String key,
                                                @QueryParam ("changeId") TimeUUIDParam changeIdParam,
                                                Map<String, Object> json,
                                                @QueryParam ("audit") AuditParam auditParam,
                                                @QueryParam ("consistency") @DefaultValue ("STRONG") WriteConsistencyParam consistency,
                                                @QueryParam ("tag") List<String> tags,
                                                @QueryParam ("debug") BooleanParam debug,
                                                @Authenticated Subject subject) {
        Set<String> tagsSet = (tags == null) ? ImmutableSet.<String>of() : Sets.newHashSet(tags);
        return doUpdate(table, key, changeIdParam, Deltas.literal(json), auditParam, consistency, debug, true, subject, tagsSet);
    }

    /**
     * Creates, modifies or deletes a piece of content in the data store by
     * applying a delta.  Expects a delta in the format produced by the
     * {@link Deltas} class and {@link Delta#toString()}.
     */
    @POST
    @Path ("{table}/{key}")
    @Consumes ("application/x.json-delta")
    @RequiresPermissions ("sor|update|{table}")
    @Timed (name = "bv.emodb.sor.DataStoreResource1.update", absolute = true)
    @ApiOperation (value = "Creates, modifies or deletes a piece of content in the data store by applying a delta.",
            notes = "Creates, modifies or deletes a piece of content in the data store by\n" +
                    " applying a delta.  Expects a delta in the format produced by the\n" +
                    " {@link Deltas} class and {@link Delta#toString()}.",
            response = SuccessResponse.class
    )
    public SuccessResponse update(@PathParam ("table") String table,
                                  @PathParam ("key") String key,
                                  @QueryParam ("changeId") TimeUUIDParam changeIdParam,
                                  String deltaString,
                                  @QueryParam ("audit") AuditParam auditParam,
                                  @QueryParam ("consistency") @DefaultValue ("STRONG") WriteConsistencyParam consistency,
                                  @QueryParam ("tag") List<String> tags,
                                  @QueryParam ("debug") BooleanParam debug,
                                  @Authenticated Subject subject) {
        checkArgument(!Strings.isNullOrEmpty(deltaString), "Missing required JSON delta request entity.");
        Set<String> tagsSet = (tags == null) ? ImmutableSet.<String>of() : Sets.newHashSet(tags);
        return doUpdate(table, key, changeIdParam, new DeltaParam(deltaString).get(), auditParam, consistency, debug,
                false, subject, tagsSet);
    }

    /**
     * Creates, modifies or deletes a piece of content in the data store by
     * applying a delta.  Expects a delta in the format produced by the
     * {@link Deltas} class and {@link Delta#toString()}.
     */
    @POST
    @Path ("_facade/{table}/{key}")
    @Consumes ("application/x.json-delta")
    @RequiresPermissions ("facade|update|{table}")
    @Timed (name = "bv.emodb.sor.DataStoreResource1.updateFacade", absolute = true)
    @ApiOperation (value = "Creates, modifies or deletes a piece of content in the data store by applying a delta.",
            notes = "Creates, modifies or deletes a piece of content in the data store by\n" +
                    " applying a delta.  Expects a delta in the format produced by the\n" +
                    " {@link Deltas} class and {@link Delta#toString()}.",
            response = SuccessResponse.class
    )
    public SuccessResponse updateFacade(@PathParam ("table") String table,
                                        @PathParam ("key") String key,
                                        @QueryParam ("changeId") TimeUUIDParam changeIdParam,
                                        String deltaString,
                                        @QueryParam ("audit") AuditParam auditParam,
                                        @QueryParam ("consistency") @DefaultValue ("STRONG") WriteConsistencyParam consistency,
                                        @QueryParam ("tag") List<String> tags,
                                        @QueryParam ("debug") BooleanParam debug,
                                        @Authenticated Subject subject) {
        checkArgument(!Strings.isNullOrEmpty(deltaString), "Missing required JSON delta request entity.");
        Set<String> tagsSet = (tags == null) ? ImmutableSet.<String>of() : Sets.newHashSet(tags);
        return doUpdate(table, key, changeIdParam, new DeltaParam(deltaString).get(), auditParam, consistency, debug,
                true, subject, tagsSet);
    }

    /**
     * Deletes a piece of content from the data store.
     */
    @DELETE
    @Path ("{table}/{key}")
    @RequiresPermissions ("sor|update|{table}")
    @Timed (name = "bv.emodb.sor.DataStoreResource1.delete", absolute = true)
    @ApiOperation (value = "Deletes a piece of content from the data store.",
            notes = "Deletes a piece of content from the data store.",
            response = SuccessResponse.class
    )
    public SuccessResponse delete(@PathParam ("table") String table,
                                  @PathParam ("key") String key,
                                  @QueryParam ("changeId") TimeUUIDParam changeIdParam,
                                  @QueryParam ("audit") AuditParam auditParam,
                                  @QueryParam ("consistency") @DefaultValue ("STRONG") WriteConsistencyParam consistency,
                                  @QueryParam ("tag") List<String> tags,
                                  @QueryParam ("debug") BooleanParam debug,
                                  @Authenticated Subject subject) {
        Set<String> tagsSet = (tags == null) ? ImmutableSet.<String>of() : Sets.newHashSet(tags);
        return doUpdate(table, key, changeIdParam, Deltas.delete(), auditParam, consistency, debug, false, subject, tagsSet);
    }

    /**
     * Deletes a piece of content from a facade in the data store.
     */
    @DELETE
    @Path ("_facade/{table}/{key}")
    @RequiresPermissions ("facade|update|{table}")
    @Timed (name = "bv.emodb.sor.DataStoreResource1.deleteFacadeContent", absolute = true)
    @ApiOperation (value = "Deletes a piece of content from a facade in the data store.",
            notes = "Deletes a piece of content from a facade in the data store.",
            response = SuccessResponse.class
    )
    public SuccessResponse deleteFacadeContent(@PathParam ("table") String table,
                                               @PathParam ("key") String key,
                                               @QueryParam ("changeId") TimeUUIDParam changeIdParam,
                                               @QueryParam ("audit") AuditParam auditParam,
                                               @QueryParam ("consistency") @DefaultValue ("STRONG") WriteConsistencyParam consistency,
                                               @QueryParam ("tag") List<String> tags,
                                               @QueryParam ("debug") BooleanParam debug,
                                               @Authenticated Subject subject) {
        Set<String> tagsSet = (tags == null) ? ImmutableSet.<String>of() : Sets.newHashSet(tags);
        return doUpdate(table, key, changeIdParam, Deltas.delete(), auditParam, consistency, debug, true, subject, tagsSet);
    }

    private SuccessResponse doUpdate(String table,
                                     String key,
                                     TimeUUIDParam changeIdParam,
                                     Delta delta,
                                     AuditParam auditParam,
                                     WriteConsistencyParam consistency,
                                     BooleanParam debug,
                                     boolean facade, Subject subject,
                                     @NotNull Set<String> tags) {
        Audit audit = getRequired(auditParam, "audit");
        UUID changeId = (changeIdParam != null) ? changeIdParam.get() : TimeUUIDs.newUUID();  // optional, defaults to new uuid

        // Perform the update
        Iterable<Update> updates = asPermissionCheckingIterable(Collections.singletonList(new Update(table, key,
                changeId, delta, audit, consistency.get())).iterator(), subject, facade);
        if (facade) {
            _dataStore.updateAllForFacade(updates, tags);
        } else {
            _dataStore.updateAll(updates, tags);
        }

        SuccessResponse response = SuccessResponse.instance();
        // In general, callers shouldn't rely on the change id being returned to do anything.
        // There's a possibility that the update succeeds but the response gets lost due to
        // network failure etc., so callers that need to know about the result of the update
        // should (1) generate the change id themselves, (2) send the update request, (3) poll
        // to see if the update took effect.  this may be slower and more complicated, but in
        // the end it's more a reliable pattern.  However, sometimes when debugging it's nice
        // to find out the change id that was chosen if one wasn't specified in the request.
        if (debug != null && debug.get()) {
            response = response.with(ImmutableMap.of("changeId", changeId));
        }
        return response;
    }

    /**
     * Imports an arbitrary size stream of an array of JSON {@link Update} objects.  In contrast to the "simpleUpdate"
     * APIs below, this method allows individual updates to have varying table changeId, audit, and consistency
     * parameters.  This makes it a good generic batch wrapper for multiple update calls, but it's awkward to use
     * directly from simple clients like curl that just want to post a bunch of JSON objects into the SoR.
     */
    @POST
    @Path ("_stream")
    @Consumes (MediaType.APPLICATION_JSON)
    @Timed (name = "bv.emodb.sor.DataStoreResource1.updateAll", absolute = true)
    @ApiOperation (value = "Does multiple update calls",
            notes = " Imports an arbitrary size stream of an array of JSON {@link Update} objects.  In contrast to the \"simpleUpdate\"\n" +
                    " APIs below, this method allows individual updates to have varying table changeId, audit, and consistency\n" +
                    " parameters.  This makes it a good generic batch wrapper for multiple update calls, but it's awkward to use\n" +
                    " directly from simple clients like curl that just want to post a bunch of JSON objects into the SoR.",
            response = SuccessResponse.class
    )
    @ApiImplicitParams ({@ApiImplicitParam (name = "APIKey", required = true)})
    public SuccessResponse updateAll(InputStream in,
                                     @QueryParam ("tag") List<String> tags,
                                     @Authenticated Subject subject) {
        Set<String> tagsSet = (tags == null) ? ImmutableSet.<String>of() : Sets.newHashSet(tags);
        Iterable<Update> updates = asPermissionCheckingIterable(new JsonStreamingArrayParser<>(in, Update.class), subject, false);
        _dataStore.updateAll(updates, tagsSet);
        return SuccessResponse.instance();
    }

    /**
     * Facade-equivalent operation for "_stream"
     * See (@link #updateAll(java.io.InputStream, BooleanParam, Subject)}
     */
    @POST
    @Path ("_facade/_stream")
    @Consumes (MediaType.APPLICATION_JSON)
    @Timed (name = "bv.emodb.sor.DataStoreResource1.updateAllForFacade", absolute = true)
    @ApiOperation (value = "Does update all for Facade",
            notes = "Facade-equivalent operation for \"_stream\"\n" +
                    " See (@link #updateAll(java.io.InputStream, BooleanParam, Subject)}",
            response = SuccessResponse.class
    )
    public SuccessResponse updateAllForFacade(InputStream in, @QueryParam ("tag") List<String> tags,
                                              @Authenticated Subject subject) {
        Set<String> tagsSet = (tags == null) ? ImmutableSet.<String>of() : Sets.newHashSet(tags);
        Iterable<Update> updates = asPermissionCheckingIterable(new JsonStreamingArrayParser<>(in, Update.class), subject, true);
        _dataStore.updateAllForFacade(updates, tagsSet);
        return SuccessResponse.instance();
    }

    /**
     * Imports an arbitrary size stream of deltas and/or JSON objects.  Two formats are supported: array syntax
     * ('[' object ',' object ',' ... ']') and whitespace-separated objects (object whitespace object whitespace ...)
     * Each piece of content must have top-level "~id" and "~table" attributes that determines the table and object
     * key in the SoR.
     */
    @POST
    @Consumes ({MediaType.APPLICATION_JSON, "application/x.json-deltas"})
    @Timed (name = "bv.emodb.sor.DataStoreResource1.simpleUpdateStream", absolute = true)
    @ApiOperation (value = "Imports an arbitrary size stream of deltas and/or JSON objects",
            notes = "Imports an arbitrary size stream of deltas and/or JSON objects.  Two formats are supported: array syntax\n" +
                    "     * ('[' object ',' object ',' ... ']') and whitespace-separated objects (object whitespace object whitespace ...)\n" +
                    "     * Each piece of content must have top-level \"~id\" and \"~table\" attributes that determines the table and object\n" +
                    "     * key in the SoR.",
            response = SuccessResponse.class
    )
    public SuccessResponse simpleUpdateStream(@QueryParam ("changeId") TimeUUIDParam changeIdParam,
                                              @QueryParam ("audit") AuditParam auditParam,
                                              @QueryParam ("consistency") @DefaultValue ("STRONG") WriteConsistencyParam consistencyParam,
                                              @QueryParam ("facade") BooleanParam facade,
                                              @Authenticated Subject subject,
                                              Reader in) {
        return doSimpleUpdateStream(Optional.<String>absent(), changeIdParam, auditParam, consistencyParam, in, facade,
                subject);
    }

    /**
     * Imports an arbitrary size stream of deltas and/or JSON objects.  Two formats are supported: array syntax
     * ('[' object ',' object ',' ... ']') and whitespace-separated objects (object whitespace object whitespace ...)
     * Each piece of content must have a top-level "~id" attribute that determines the object key in the SoR.
     */
    @POST
    @Path ("{table}")
    @Consumes ({MediaType.APPLICATION_JSON, "application/x.json-deltas"})
    @Timed (name = "bv.emodb.sor.DataStoreResource1.simpleUpdateTableStream", absolute = true)
    @ApiOperation (value = "Imports an arbitrary size stream of deltas and/or JSON objects.",
            notes = " Imports an arbitrary size stream of deltas and/or JSON objects.  Two formats are supported: array syntax\n" +
                    " ('[' object ',' object ',' ... ']') and whitespace-separated objects (object whitespace object whitespace ...)\n" +
                    " Each piece of content must have a top-level \"~id\" attribute that determines the object key in the SoR.",
            response = SuccessResponse.class
    )
    public SuccessResponse simpleUpdateTableStream(@PathParam ("table") final String table,
                                                   @QueryParam ("changeId") TimeUUIDParam changeIdParam,
                                                   @QueryParam ("audit") AuditParam auditParam,
                                                   @QueryParam ("consistency") @DefaultValue ("STRONG") WriteConsistencyParam consistencyParam,
                                                   @QueryParam ("facade") BooleanParam facade,
                                                   @QueryParam ("tag") List<String> tags,
                                                   @Authenticated Subject subject,
                                                   Reader in) {
        return doSimpleUpdateStream(Optional.of(table), changeIdParam, auditParam, consistencyParam, in, facade,
                subject);
    }

    private SuccessResponse doSimpleUpdateStream(final Optional<String> tableParam, final TimeUUIDParam changeIdParam,
                                                 AuditParam auditParam, WriteConsistencyParam consistencyParam, Reader in,
                                                 BooleanParam facade, Subject subject) {
        final Audit audit = getRequired(auditParam, "audit");
        final WriteConsistency consistency = consistencyParam.get();

        Iterator<Update> updates = Iterators.transform(Deltas.fromStream(in), new Function<Delta, Update>() {
            @Override
            public Update apply(Delta delta) {
                String table = tableParam.isPresent() ? tableParam.get() : extractKey(delta, Intrinsic.TABLE, String.class);
                checkArgument(table != null, "JSON object is missing field required by streaming update: %s", Intrinsic.TABLE);

                String key = extractKey(delta, Intrinsic.ID, String.class);
                checkArgument(key != null, "JSON object is missing field required by streaming update: %s", Intrinsic.ID);

                UUID changeId = (changeIdParam != null) ? changeIdParam.get() : TimeUUIDs.newUUID();  // optional, defaults to new uuid

                return new Update(table, key, changeId, delta, audit, consistency);
            }
        });

        if (facade != null && facade.get()) {
            _dataStore.updateAllForFacade(asPermissionCheckingIterable(updates, subject, true));
        } else {
            // Parse and iterate through the deltas such that we never hold all the deltas in memory at once.
            _dataStore.updateAll(asPermissionCheckingIterable(updates, subject, false));
        }

        return SuccessResponse.instance();
    }

    private Iterable<Update> asPermissionCheckingIterable(Iterator<Update> updates, final Subject subject, final boolean isFacade) {
        return Iterables.filter(
                OneTimeIterable.wrap(updates),
                new Predicate<Update>() {
                    @Override
                    public boolean apply(Update update) {
                        NamedResource resource = new NamedResource(update.getTable());
                        boolean hasPermission;
                        if (isFacade) {
                            hasPermission = subject.hasPermission(Permissions.updateFacade(resource));
                        } else {
                            hasPermission = subject.hasPermission(Permissions.updateSorTable(resource));
                        }

                        if (!hasPermission) {
                            throw new UnauthorizedException("not authorized to update table " + update.getTable());
                        }
                        return true;
                    }
                });
    }

    /**
     * Attempts to reduce the size of the specified content in the underlying storage.
     * Normally compaction occurs automatically as a side effect of performing a
     * {@link #get}, but it can be forced to occur using this method.
     */
    @POST
    @Path ("{table}/{key}/compact")
    @RequiresPermissions ("sor|compact|{table}")
    @Timed (name = "bv.emodb.sor.DataStoreResource1.compact", absolute = true)
    @ApiOperation (value = "Attempts to reduce the size of the specified content in the underlying storage.",
            notes = "Attempts to reduce the size of the specified content in the underlying storage.\n" +
                    " Normally compaction occurs automatically as a side effect of performing a\n" +
                    " {@link #get}, but it can be forced to occur using this method.",
            response = SuccessResponse.class
    )
    public SuccessResponse compact(@PathParam ("table") String table,
                                   @PathParam ("key") String key,
                                   @QueryParam ("ttl") SecondsParam ttlParam,
                                   @QueryParam ("readConsistency") @DefaultValue ("STRONG") ReadConsistencyParam readConsistency,
                                   @QueryParam ("writeConsistency") @DefaultValue ("STRONG") WriteConsistencyParam writeConsistency) {
        // Weak consistency should work, but presumably the caller is debugging/testing so the
        // performance hit of strong consistency is worth it so default to strong consistency
        Duration ttl = getOptional(ttlParam);
        _dataStore.compact(table, key, ttl, readConsistency.get(), writeConsistency.get());
        return SuccessResponse.instance();
    }

    /**
     * Returns the stash root directory for this cluster, or an HTTP not found if stash is not supported by this server.
     */
    @GET
    @Path ("_stashroot")
    @Produces (MediaType.TEXT_PLAIN)
    @ApiOperation (value = "Returns the stash root directory for this cluster.",
            notes = "Returns the stash root directory for this cluster, or an HTTP not found if stash is not supported by this server.",
            response = String.class
    )
    public String stashRoot() {
        return _dataStore.getStashRoot().toString();
    }

    private Response redirectTo(DataCenter dataCenter, URI requestUri) {
        // Use the scheme+authority from the data center and the path+query from the request uri
        URI location = UriBuilder.fromUri(dataCenter.getServiceUri()).
                replacePath(requestUri.getRawPath()).
                replaceQuery(requestUri.getRawQuery()).
                build();
        return Response.status(Response.Status.MOVED_PERMANENTLY).
                location(location).
                header("X-BV-Exception", UnsupportedOperationException.class.getName()).
                build();
    }

    private UUID parseUuidOrTimestamp(@Nullable String string, boolean rangeUpperEnd) {
        if (string == null) {
            return null;
        } else if (UUID_LIKE_PATTERN.matcher(string).matches()) {
            return new TimeUUIDParam(string).get();
        } else {
            // Timestamps have a granularity of a millisecond so adjust upper endpoints to be
            // the last valid time UUID for the millisecond.
            Instant date = new InstantParam(string).get();
            if (rangeUpperEnd) {
                return TimeUUIDs.getPrevious(TimeUUIDs.uuidForTimeMillis(date.toEpochMilli() + 1));
            } else {
                return TimeUUIDs.uuidForTimeMillis(date.toEpochMilli());
            }
        }
    }

    private List<Coordinate> parseCoordinates(List<String> coordinates) {
        return Lists.transform(coordinates,
                new Function<String, Coordinate>() {
                    @Override
                    public Coordinate apply(String input) {
                        return Coordinate.parse(input);
                    }
                });
    }

    private static <T> T extractKey(Delta delta, String key, Class<T> type) {
        if (delta instanceof Literal) {
            Object json = ((Literal) delta).getValue();
            if (json instanceof Map) {
                return cast(((Map) json).get(key), type);
            }
        } else if (delta instanceof MapDelta) {
            Delta value = ((MapDelta) delta).getEntries().get(key);
            if (value instanceof Literal) {
                return cast(((Literal) value).getValue(), type);
            }
        }
        throw new IllegalArgumentException(format("Unable to extract top-level key \"%s\" from delta: %s", key, delta));
    }

    private static <T> T cast(Object value, Class<T> type) {
        if (!type.isInstance(value)) {
            throw new IllegalArgumentException(format("Unable to cast value to %s: %s", type.getSimpleName(), value));
        }
        return type.cast(value);
    }

    private static <T> T getRequired(AbstractParam<T> param, String name) {
        if (param == null) {
            throw new IllegalArgumentException(format("Missing required query parameter: %s", name));
        }
        return param.get();
    }

    private <T> T getOptional(AbstractParam<T> param) {
        return (param != null) ? param.get() : null;
    }

    private static <T> Iterator<T> streamingIterator(Iterator<T> iterator, BooleanParam debug) {
        // If debugging, sort the individual json objects so they're easier to understand in a browser
        iterator = optionallyOrdered(iterator, debug);

        // Force the calculation of at least the first item in the iterator so that, if an exception occurs, we find
        // out before writing the HTTP response code & headers.  Otherwise we will at best report a 500 error instead
        // of applying Jersey exception mappings and maybe returning a 400 error etc.
        PeekingIterator<T> peekingIterator = Iterators.peekingIterator(iterator);
        if (peekingIterator.hasNext()) {
            peekingIterator.peek();
        }

        return new LoggingIterator<>(peekingIterator, _log);
    }

    private static <T> T optionallyOrdered(T content, BooleanParam debug) {
        if (debug != null && debug.get()) {
            //noinspection unchecked
            content = (T) OrderedJson.ordered(content);
        }
        return content;
    }

    private static <T> Iterator<T> optionallyOrdered(Iterator<T> iterator, BooleanParam debug) {
        if (debug != null && debug.get()) {
            iterator = Iterators.transform(iterator, new Function<T, T>() {
                @Override
                public T apply(@Nullable T content) {
                    //noinspection unchecked
                    return (T) OrderedJson.ordered(content);
                }
            });
        }
        return iterator;
    }

    private static FilteredJsonStreamingOutput<Map<String, Object>> deletedContentFilteringStream(Iterator<Map<String, Object>> iterator, long limit) {
        return new FilteredJsonStreamingOutput<Map<String, Object>>(iterator, limit) {
            @Override
            public boolean include(Map<String, Object> value) {
                return !Intrinsic.isDeleted(value);
            }
        };
    }
}
