package com.bazaarvoice.emodb.web.resources.blob;

import com.bazaarvoice.emodb.auth.jersey.Authenticated;
import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.blob.api.Blob;
import com.bazaarvoice.emodb.blob.api.BlobMetadata;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.blob.api.DefaultBlobMetadata;
import com.bazaarvoice.emodb.blob.api.DefaultTable;
import com.bazaarvoice.emodb.blob.api.Range;
import com.bazaarvoice.emodb.blob.api.RangeSpecification;
import com.bazaarvoice.emodb.blob.api.Table;
import com.bazaarvoice.emodb.common.api.UnauthorizedException;
import com.bazaarvoice.emodb.common.json.LoggingIterator;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.web.auth.Permissions;
import com.bazaarvoice.emodb.web.auth.resource.CreateTableResource;
import com.bazaarvoice.emodb.web.auth.resource.NamedResource;
import com.bazaarvoice.emodb.web.jersey.params.SecondsParam;
import com.bazaarvoice.emodb.web.resources.SuccessResponse;
import com.bazaarvoice.emodb.web.resources.sor.AuditParam;
import com.bazaarvoice.emodb.web.resources.sor.TableOptionsParam;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.google.common.io.InputSupplier;
import com.sun.jersey.api.client.ClientResponse;
import io.dropwizard.jersey.params.AbstractParam;
import io.dropwizard.jersey.params.LongParam;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.shiro.authz.annotation.RequiresAuthentication;
import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.coursera.metrics.datadog.TaggedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

import static com.bazaarvoice.emodb.blob.core.Constants.STORAGE_ATTRIBUTE_NAME;
import static java.lang.String.format;

@Path("/blob/1")
@Produces(MediaType.APPLICATION_JSON)
@RequiresAuthentication
@Api (value="BlobStore: " , description = "All BlobStore operations")
public class BlobStoreResource1 {
    private static final Logger _log = LoggerFactory.getLogger(BlobStoreResource1.class);

    private static final String X_BV_PREFIX = "X-BV-";    // HTTP header prefix for BlobMetadata other than attributes
    private static final String X_BVA_PREFIX = "X-BVA-";  // HTTP header prefix for BlobMetadata attributes
    private static final Pattern CONTENT_ENCODING = Pattern.compile("content[-_]?encoding", Pattern.CASE_INSENSITIVE);
    private static final Pattern CONTENT_TYPE = Pattern.compile("content[-_]?type", Pattern.CASE_INSENSITIVE);
    private static final String METRIC_PREFIX = "bv.emodb.blob.BlobStoreResource1.";

    private final BlobStore _blobStore;
    private final Set<String> _approvedContentTypes;

    private final MetricRegistry _metricRegistry;
    private final LoadingCache<String, Meter> _listTableRequestsByApiKey;
    private final LoadingCache<String, Meter> _createTableRequestsByApiKey;
    private final LoadingCache<String, Meter> _dropTableRequestsByApiKey;
    private final LoadingCache<String, Meter> _purgeTableRequestsByApiKey;
    private final LoadingCache<String, Meter> _getTableAttributesRequestsByApiKey;
    private final LoadingCache<String, Meter> _setTableAttributesRequestsByApiKey;
    private final LoadingCache<String, Meter> _getTableOptionsRequestsByApiKey;
    private final LoadingCache<String, Meter> _getTableSizeRequestsByApiKey;
    private final LoadingCache<String, Meter> _getTableMetadataRequestsByApiKey;
    private final LoadingCache<String, Meter> _getObjectMetadataRequestsByApiKey;
    private final LoadingCache<String, Meter> _scanMetadataRequestsByApiKey;
    private final LoadingCache<String, Meter> _getTablePlacementsRequestsByApiKey;
    private final LoadingCache<String, Meter> _getObjectRequestsByApiKey;
    private final LoadingCache<String, Meter> _putObjectRequestsByApiKey;
    private final LoadingCache<String, Meter> _deleteObjectRequestsByApiKey;

    public BlobStoreResource1(BlobStore blobStore, Set<String> approvedContentTypes, MetricRegistry metricRegistry) {
        _blobStore = blobStore;
        _approvedContentTypes = approvedContentTypes;
        _metricRegistry = metricRegistry;

        _listTableRequestsByApiKey = createMetricCache("listTablesByApiKey");
        _createTableRequestsByApiKey = createMetricCache("createTableByApiKey");
        _dropTableRequestsByApiKey = createMetricCache("dropTableByApiKey");
        _purgeTableRequestsByApiKey = createMetricCache("purgeTableByApiKey");
        _getTableAttributesRequestsByApiKey = createMetricCache("getTableAttributesByApiKey");
        _setTableAttributesRequestsByApiKey = createMetricCache("setTableAttributesByApiKey");
        _getTableOptionsRequestsByApiKey = createMetricCache("getTableOptionsByApiKey");
        _getTableSizeRequestsByApiKey = createMetricCache("getTableSizeByApiKey");
        _getTableMetadataRequestsByApiKey = createMetricCache("getTableMetadataByApiKey");
        _getObjectMetadataRequestsByApiKey = createMetricCache("headByApiKey");
        _scanMetadataRequestsByApiKey = createMetricCache("scanMetadataByApiKey");
        _getTablePlacementsRequestsByApiKey = createMetricCache("getTablePlacementsByApiKey");
        _getObjectRequestsByApiKey = createMetricCache("getByApiKey");
        _putObjectRequestsByApiKey = createMetricCache("putByApiKey");
        _deleteObjectRequestsByApiKey = createMetricCache("deleteByApiKey");

    }

    private LoadingCache<String, Meter> createMetricCache(String metricName) {
        return CacheBuilder.newBuilder()
                .build(new CacheLoader<String, Meter>() {
                    @Override
                    public Meter load(String key) throws Exception {
                        String metric = new TaggedName.TaggedNameBuilder()
                                .metricName(METRIC_PREFIX + metricName)
                                .addTag("apiKey", key)
                                .build()
                                .encode();
                        return _metricRegistry.meter(metric);
                    }
                });
    }

    @GET
    @Path("_table")
    @Timed(name = "bv.emodb.blob.BlobStoreResource1.listTables", absolute = true)
    @ApiOperation (value = "List all the tables.",
            notes = "Returns a list of tables.",
            response = Table.class
    )
    public Iterator<Table> listTables(@QueryParam("from") final String fromKeyExclusive,
                                      @QueryParam("limit") @DefaultValue("10") LongParam limit,
                                      @QueryParam("includeStorageAttribute") @DefaultValue("false") Boolean includeStorageAttribute,
                                      @Authenticated Subject subject) {
        _listTableRequestsByApiKey.getUnchecked(subject.getId()).mark();
        Iterator<Table> iterator = StreamSupport.stream(Spliterators.spliteratorUnknownSize(_blobStore.listTables(Strings.emptyToNull(fromKeyExclusive), Long.MAX_VALUE), 0), false)
                .filter(input -> subject.hasPermission(Permissions.readBlobTable(new NamedResource(input.getName()))))
                .limit(limit.get())
                .iterator();
        if (includeStorageAttribute) {
            return streamingIterator(iterator);
        } else {
            return streamingIterator(Iterators.transform(iterator,
                    md -> {
                        if (md.getAttributes().containsKey(STORAGE_ATTRIBUTE_NAME)) {
                            return new DefaultTable(md.getName(), md.getOptions(), getAttributesWithoutStorage(md.getAttributes()), md.getAvailability());
                        } else {
                            return md;
                        }
                    }));
        }
    }

    private Map<String, String> getAttributesWithoutStorage(Map<String, String> attributes) {
        Map<String, String> attributesMap = new HashMap<>();
        attributesMap.putAll(attributes);
        attributesMap.remove(STORAGE_ATTRIBUTE_NAME);
        return attributesMap;
    }

    @PUT
    @Path("_table/{table}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Timed(name = "bv.emodb.blob.BlobStoreResource1.createTable", absolute = true)
    @ApiOperation (value = "Creates a table.",
            notes = "Returns a SuccessResponse if table is created.",
            response = SuccessResponse.class
    )
    public SuccessResponse createTable(@PathParam("table") String table,
                                       @QueryParam("options") TableOptionsParam optionParams,
                                       Map<String, String> attributes,
                                       @QueryParam("audit") AuditParam auditParam,
                                       @Context UriInfo uriInfo,
                                       @Authenticated Subject subject) {
        _createTableRequestsByApiKey.getUnchecked(subject.getId()).mark();
        TableOptions options = getRequired(optionParams, "options");
        Audit audit = getRequired(auditParam, "audit");

        // Check permission for creating this table
        CreateTableResource resource = new CreateTableResource(table, options.getPlacement(), attributes);

        if (!subject.hasPermission(Permissions.createBlobTable(resource))) {
            throw new UnauthorizedException();
        }

        _blobStore.createTable(table, options, attributes, audit);
        return SuccessResponse.instance();
    }

    @DELETE
    @Path("_table/{table}")
    @RequiresPermissions("blob|drop_table|{table}")
    @Timed(name = "bv.emodb.blob.BlobStoreResource1.dropTable", absolute = true)
    @ApiOperation (value = "Drops a table.",
            notes = "Returns a SucessResponse if the table is dropped.",
            response = SuccessResponse.class
    )
    public SuccessResponse dropTable(@PathParam("table") String table,
                                     @QueryParam("audit") AuditParam auditParam,
                                     @Context UriInfo uriInfo,
                                     @Authenticated Subject subject) {
        _dropTableRequestsByApiKey.getUnchecked(subject.getId()).mark();
        Audit audit = getRequired(auditParam, "audit");
        _blobStore.dropTable(table, audit);
        return SuccessResponse.instance();
    }

    @POST
    @Path("_table/{table}/purge")
    @RequiresPermissions("blob|purge|{table}")
    @Timed(name = "bv.emodb.blob.BlobStoreResource1.purgeTable", absolute = true)
    @ApiOperation (value = "Purges a table.",
            notes = "Returns a SucessResponse if the table is purged..",
            response = SuccessResponse.class
    )
    public SuccessResponse purgeTable(@PathParam("table") String table,
                                      @QueryParam("audit") AuditParam auditParam,
                                      @Authenticated Subject subject) {
        _purgeTableRequestsByApiKey.getUnchecked(subject.getId()).mark();
        Audit audit = getRequired(auditParam, "audit");
        _blobStore.purgeTableUnsafe(table, audit);
        return SuccessResponse.instance();
    }

    @GET
    @Path("_table/{table}")
    @RequiresPermissions("blob|read|{table}")
    @Timed(name = "bv.emodb.blob.BlobStoreResource1.getTableAttributes", absolute = true)
    @ApiOperation (value = "Gets all the attributes of a table.",
            notes = "Returns a Map",
            response = Map.class
    )
    public Map<String, String> getTableAttributes(@PathParam("table") String table,
                                                  @QueryParam("includeStorageAttribute") @DefaultValue("false") Boolean includeStorageAttribute,
                                                  @Authenticated Subject subject) {
        _getTableAttributesRequestsByApiKey.getUnchecked(subject.getId()).mark();
        Map<String, String> tableAttributes = _blobStore.getTableAttributes(table);

        if (includeStorageAttribute || !tableAttributes.containsKey(STORAGE_ATTRIBUTE_NAME)) {
           return tableAttributes;
        } else {
            return getAttributesWithoutStorage(tableAttributes);
        }
    }

    @PUT
    @Path("_table/{table}/attributes")
    @Consumes(MediaType.APPLICATION_JSON)
    @RequiresPermissions("blob|set_table_attributes|{table}")
    @Timed(name = "bv.emodb.blob.BlobStoreResource1.setTableAttributes", absolute = true)
    @ApiOperation (value = "Sets the attibutes for a table.",
            notes = "Returns a SucessResponse if the attributes are set.",
            response = SuccessResponse.class
    )
    public SuccessResponse setTableAttributes(@PathParam("table") String table,
                                              Map<String, String> attributes,
                                              @QueryParam("audit") AuditParam auditParam,
                                              @Context UriInfo uriInfo,
                                              @Authenticated Subject subject) {
        _setTableAttributesRequestsByApiKey.getUnchecked(subject.getId()).mark();
        Audit audit = getRequired(auditParam, "audit");
        _blobStore.setTableAttributes(table, attributes, audit);
        return SuccessResponse.instance();
    }

    @GET
    @Path("_table/{table}/options")
    @RequiresPermissions("blob|read|{table}")
    @Timed(name = "bv.emodb.blob.BlobStoreResource1.getTableOptions", absolute = true)
    @ApiOperation (value = "Gets the options of the table.",
            notes = "Returns TableOptions object.",
            response = TableOptions.class
    )
    public TableOptions getTableOptions(@PathParam("table") String table,
                                        @Authenticated Subject subject) {
        _getTableOptionsRequestsByApiKey.getUnchecked(subject.getId()).mark();
        return _blobStore.getTableOptions(table);
    }

    @GET
    @Path("_table/{table}/size")
    @RequiresPermissions("blob|read|{table}")
    @Timed(name = "bv.emodb.blob.BlobStoreResource1.getTableSize", absolute = true)
    @ApiOperation (value = "Gets the size of the table.",
            notes = "Retuns a long.",
            response = long.class
    )
    public long getTableSize(@PathParam("table") String table,
                             @Authenticated Subject subject) {
        _getTableSizeRequestsByApiKey.getUnchecked(subject.getId()).mark();
        return _blobStore.getTableApproximateSize(table);
    }

    @GET
    @Path ("_table/{table}/metadata")
    @RequiresPermissions("blob|read|{table}")
    @Timed(name = "bv.emodb.blob.BlobStoreResource1.getTableMetadata", absolute = true)
    @ApiOperation (value = "Gets metadata of the table.",
            notes = "Returns a Table object.",
            response = Table.class
    )
    public Table getTableMetadata(@PathParam ("table") String table,
                                  @QueryParam("includeStorageAttribute") @DefaultValue("false") Boolean includeStorageAttribute,
                                  @Authenticated Subject subject) {
        _getTableMetadataRequestsByApiKey.getUnchecked(subject.getId()).mark();
        Table tableMetadata = _blobStore.getTableMetadata(table);
        if (includeStorageAttribute || !tableMetadata.getAttributes().containsKey(STORAGE_ATTRIBUTE_NAME)) {
            return tableMetadata;
        } else {
            Map<String, String> attributes = getAttributesWithoutStorage(tableMetadata.getAttributes());
            return new DefaultTable(tableMetadata.getName(), tableMetadata.getOptions(), attributes, tableMetadata.getAvailability());
        }
    }

    /**
     * Retrieves the current version of a piece of content from the data store.
     */
    @HEAD
    @Path("{table}/{blobId}")
    @RequiresPermissions("blob|read|{table}")
    @Timed(name = "bv.emodb.blob.BlobStoreResource1.head", absolute = true)
    @ApiOperation (value = "Retrieves the current version of a piece of content from the data store.",
            notes = "Returns a response object.",
            response = Response.class
    )
    public Response head(@PathParam("table") String table,
                         @PathParam("blobId") String blobId,
                         @QueryParam("includeStorageAttribute") @DefaultValue("false") Boolean includeStorageAttribute,
                         @Authenticated Subject subject) {
        _getObjectMetadataRequestsByApiKey.getUnchecked(subject.getId()).mark();
        BlobMetadata blob = _blobStore.getMetadata(table, blobId);

        Response.ResponseBuilder response = Response.ok();
        setHeaders(response, blob, null, includeStorageAttribute);
        return response.build();
    }

    /**
     * Retrieves a list of content items in a particular table.
     */
    @GET
    @Path("{table}")
    @RequiresPermissions("blob|read|{table}")
    @Timed(name = "bv.emodb.blob.BlobStoreResource1.scanMetadata", absolute = true)
    @ApiOperation (value = "Retrieves a list of content items in a particular table.",
            notes = "Retuns BlobMetadata.",
            response = BlobMetadata.class
    )
    public Iterator<BlobMetadata> scanMetadata(@PathParam("table") String table,
                                               @QueryParam("from") String blobId,
                                               @QueryParam("limit") @DefaultValue("10") LongParam limit,
                                               @QueryParam("includeStorageAttribute") @DefaultValue("false") Boolean includeStorageAttribute,
                                               @Authenticated Subject subject) {
        _scanMetadataRequestsByApiKey.getUnchecked(subject.getId()).mark();

        Iterator<BlobMetadata> iterator = _blobStore.scanMetadata(table, Strings.emptyToNull(blobId), limit.get());
        if (includeStorageAttribute) {
            return streamingIterator(iterator);
        } else {
            return streamingIterator(Iterators.transform(iterator, md -> {
                if (!md.getAttributes().containsKey(STORAGE_ATTRIBUTE_NAME)) {
                    return md;
                } else {
                    return new DefaultBlobMetadata(md.getId(), md.getTimestamp(), md.getLength(), md.getMD5(), md.getSHA1(), getAttributesWithoutStorage(md.getAttributes()));
                }
            }));
        }
    }

    /** Returns a list of valid table placements. */
    @GET
    @Path("_tableplacement")
    @Timed(name = "bv.emodb.blob.BlobStoreResource1.getTablePlacements", absolute = true)
    @ApiOperation (value = "Returns a list of valid table placements.",
            notes = "Retuns a Collection of strings.",
            response = String.class
    )
    public Collection<String> getTablePlacements(@Authenticated Subject subject) {
        _getTablePlacementsRequestsByApiKey.getUnchecked(subject.getId()).mark();
        return _blobStore.getTablePlacements();
    }


    /**
     * Retrieves the current version of a piece of content from the data store.
     */
    @GET
    @Path("{table}/{blobId}")
    @RequiresPermissions("blob|read|{table}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Timed(name = "bv.emodb.blob.BlobStoreResource1.get", absolute = true)
    @ApiOperation (value = "Retrieves the current version of a piece of content from the data store..",
            notes = "Returns a Response.",
            response = Response.class
    )
    public Response get(@PathParam("table") String table,
                        @PathParam("blobId") String blobId,
                        @HeaderParam("Range") RangeParam rangeParam,
                        @QueryParam("includeStorageAttribute") @DefaultValue("false") Boolean includeStorageAttribute,
                        @Authenticated Subject subject) {
        _getObjectRequestsByApiKey.getUnchecked(subject.getId()).mark();
        RangeSpecification rangeSpec = rangeParam != null ? rangeParam.get() : null;
        final Blob blob = _blobStore.get(table, blobId, rangeSpec);

        Response.ResponseBuilder response = Response.ok(new StreamingOutput() {
            @Override
            public void write(OutputStream output) throws IOException {
                blob.writeTo(output);
            }
        });
        setHeaders(response, blob, (rangeSpec != null) ? blob.getByteRange() : null, includeStorageAttribute);
        return response.build();
    }

    private void setHeaders(Response.ResponseBuilder response, BlobMetadata metadata, Range range, boolean includeStorageAttribute) {
        Map<String, String> attributes = metadata.getAttributes();
        if (!includeStorageAttribute) {
            attributes = getAttributesWithoutStorage(attributes);
        }

        // Set the length so the HTTP client knows how many bytes to expect in the response
        if (range == null) {
            response.header(HttpHeaders.CONTENT_LENGTH, metadata.getLength());
        } else {
            response.status(ClientResponse.Status.PARTIAL_CONTENT);
            response.header(HttpHeaders.CONTENT_LENGTH, range.getLength());
            response.header("Content-Range", "bytes " + range.getOffset() + "-" +
                    (range.getOffset() + range.getLength() - 1) + "/" + metadata.getLength());
        }

        response.lastModified(metadata.getTimestamp());

        // Put the MD5 in Content-MD5 (http spec says must be base64), SHA1 hash in ETAG (use hex)
        response.header(com.google.common.net.HttpHeaders.CONTENT_MD5, hexToBase64(metadata.getMD5()));
        response.header(HttpHeaders.ETAG, '"' + metadata.getSHA1() + '"');

        // Default to a binary "Content-Type" header.
        response.type(MediaType.APPLICATION_OCTET_STREAM_TYPE);

        // Set the length in a separate header that won't be munged by byte ranges, proxies, gzip compression etc.
        response.header(X_BV_PREFIX + "Length", metadata.getLength());

        // Copy all of the attributes to X-BVA- headers that don't conflict with standard HTTP headers
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            String name = entry.getKey();
            String value = entry.getValue();
            response.header(X_BVA_PREFIX + name, value);

            if (CONTENT_TYPE.matcher(name).matches()) {
                // Set the content type so browsers, etc. can display the content natively
                response.type(safeResponseContentType(value));
            } else if (CONTENT_ENCODING.matcher(name).matches()) {
                // Set the content encoding so browsers etc. can uncompress the content automatically, if necessary
                response.header(HttpHeaders.CONTENT_ENCODING, value);
            }
        }
    }

    /**
     * Because Emo sets the Content-Type header based on the X-BVA content type header it's possible for a client to
     * maliciously abuse the system.  For example, a client may create a blob at "table/evil.png" with content
     * type "text/html" and content akin to:
     * <code>
     *     &lt;html&gt;
     *         &lt;script&gt;
     *             doSomethingBad();
     *         &lt;/script&gt;
     *     &lt;/html&gt;
     * </code>
     *
     * To prevent this we always pass back the X-BVA content type as provided but only set the HTTP response's
     * "Content-Type" header if it matches an approved set of safe types.
     */
    private String safeResponseContentType(String metadataContentType) {
        if (_approvedContentTypes.contains(metadataContentType)) {
            return metadataContentType;
        }
        // Content type is not safe.  Use binary.
        return MediaType.APPLICATION_OCTET_STREAM;
    }

    @PUT
    @Path("{table}/{blobId}")
    @Consumes(MediaType.WILDCARD)
    @RequiresPermissions("blob|update|{table}")
    @Timed(name = "bv.emodb.blob.BlobStoreResource1.put", absolute = true)
    @ApiOperation (value = "Put operation.",
            notes = "Returns a SuccessReponse on success.",
            response = SuccessResponse.class
    )
    public SuccessResponse put(@PathParam("table") String table,
                               @PathParam("blobId") String blobId,
                               InputStream in,
                               @QueryParam("ttl") SecondsParam ttlParam,
                               @Context HttpHeaders headers,
                               @Authenticated Subject subject)
            throws IOException {
        _putObjectRequestsByApiKey.getUnchecked(subject.getId()).mark();
        // Note: we could copy the Content-Type and Content-Encoding headers into the attributes automatically because
        // they're so common, but in practice this runs into two problems: (1) Dropwizard interprets Content-Encoding
        // and automatically uncompresses gzip uploads, which generally isn't what we want, and (2) curl sets the
        // Content-Type to "application/x-www-form-urlencoded" by default and that's almost never what we want.
        // So, there are two special headers a user can set:
        //   X-BVA-contentEncoding:  the value of this attribute will be copied to Content-Encoding on GET
        //   X-BVA-contentType: the value of this attribute will be copied to Content-Type on GET

        // Copy all the "X-BVA-*" headers into the attributes
        Map<String, String> attributes = Maps.newHashMap();
        for (Map.Entry<String, List<String>> entry : headers.getRequestHeaders().entrySet()) {
            if (entry.getKey().startsWith(X_BVA_PREFIX)) {
                attributes.put(entry.getKey().substring(X_BVA_PREFIX.length()), entry.getValue().get(0));
            }
        }

        // The "ttl" query param can be specified to delete the blob automatically after a period of time
        Duration ttl = (ttlParam != null) ? ttlParam.get() : null;
        if (null != ttl) {
            throw new IllegalArgumentException(String.format("Ttl:{} is specified for blobId:{}"));
        }

        // Perform the put
        _blobStore.put(table, blobId, onceOnlySupplier(in), attributes);

        return SuccessResponse.instance();
    }

    @DELETE
    @Path("{table}/{blobId}")
    @RequiresPermissions("blob|update|{table}")
    @Timed(name = "bv.emodb.blob.BlobStoreResource1.delete", absolute = true)
    @ApiOperation (value = "Delete operation.",
            notes = "Returns SuccessReponse.",
            response = SuccessResponse.class
    )
    public SuccessResponse delete(@PathParam("table") String table,
                                  @PathParam("blobId") String blobId,
                                  @Authenticated Subject subject) {
        _deleteObjectRequestsByApiKey.getUnchecked(subject.getId()).mark();
        _blobStore.delete(table, blobId);
        return SuccessResponse.instance();
    }

    private String hexToBase64(String hex) {
        try {
            return Base64.encodeBase64String(Hex.decodeHex(hex.toCharArray()));
        } catch (DecoderException e) {
            return null;
        }
    }

    private <T> T getRequired(AbstractParam<T> value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(format("Missing required query parameter: %s", name));
        }
        return value.get();
    }

    private static <T> Iterator<T> streamingIterator(Iterator<T> iterator) {
        // Force the calculation of at least the first item in the iterator so that, if an exception occurs, we find
        // out before writing the HTTP response code & headers.  Otherwise we will at best report a 500 error instead
        // of applying Jersey exception mappings and maybe returning a 400 error etc.
        PeekingIterator<T> peekingIterator = Iterators.peekingIterator(iterator);
        if (peekingIterator.hasNext()) {
            peekingIterator.peek();
        }

        return new LoggingIterator<>(peekingIterator, _log);
    }

    /**
     * Returns an InputSupplier that throws an exception if the caller attempts to consume the input stream
     * multiple times.
     */
    private InputSupplier<InputStream> onceOnlySupplier(final InputStream in) {
        final AtomicBoolean once = new AtomicBoolean();
        return new InputSupplier<InputStream>() {
            @Override
            public InputStream getInput() throws IOException {
                if (!once.compareAndSet(false, true)) {
                    throw new IllegalStateException("Input stream may be consumed only once per BlobStore call.");
                }
                return in;
            }
        };
    }
}
