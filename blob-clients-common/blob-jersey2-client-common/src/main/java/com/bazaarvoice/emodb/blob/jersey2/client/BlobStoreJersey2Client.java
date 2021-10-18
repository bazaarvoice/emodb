package com.bazaarvoice.emodb.blob.jersey2.client;

import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.bazaarvoice.emodb.blob.api.AuthBlobStore;
import com.bazaarvoice.emodb.blob.api.Blob;
import com.bazaarvoice.emodb.blob.api.BlobMetadata;
import com.bazaarvoice.emodb.blob.api.BlobNotFoundException;
import com.bazaarvoice.emodb.blob.api.DefaultBlob;
import com.bazaarvoice.emodb.blob.api.DefaultBlobMetadata;
import com.bazaarvoice.emodb.blob.api.Range;
import com.bazaarvoice.emodb.blob.api.RangeNotSatisfiableException;
import com.bazaarvoice.emodb.blob.api.RangeSpecification;
import com.bazaarvoice.emodb.blob.api.StreamSupplier;
import com.bazaarvoice.emodb.blob.api.Table;
import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.client.EmoClientException;
import com.bazaarvoice.emodb.client.EmoResource;
import com.bazaarvoice.emodb.client.EmoResponse;
import com.bazaarvoice.emodb.client.uri.EmoUriBuilder;
import com.bazaarvoice.emodb.common.api.ServiceUnavailableException;
import com.bazaarvoice.emodb.common.api.UnauthorizedException;
import com.bazaarvoice.emodb.common.json.RisonHelper;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.TableExistsException;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.UnknownPlacementException;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.net.HttpHeaders;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;

import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Blob store client implementation that routes API calls to the EmoDB service.  The actual HTTP communication
 * is managed by the {@link EmoClient} implementation to allow for flexible usage by variety of HTTP client
 * implementations, such as Jersey.
 */
//TODO add documentation for using the client once it is validated
public class BlobStoreJersey2Client implements AuthBlobStore {

    /**
     * Must match the service name in the EmoService class.
     */
    /*package*/ static final String BASE_SERVICE_NAME = "emodb-blob-1";

    /**
     * Must match the @Path annotation on the BlobStoreResource class.
     */
    public static final String SERVICE_PATH = "/blob/1";

    private static final String X_BV_PREFIX = "X-BV-";    // HTTP header prefix for BlobMetadata other than attributes
    private static final String X_BVA_PREFIX = "X-BVA-";  // HTTP header prefix for BlobMetadata attributes

    /**
     * Regex for parsing the HTTP Content-Range header.
     */
    private static final Pattern CONTENT_RANGE_PATTERN = Pattern.compile("^bytes (\\d+)-(\\d+)/\\d+$");

    private static final int HTTP_PARTIAL_CONTENT = 206;

    /**
     * Delay after which streaming connections are automatically closed if the caller doesn't begin reading the stream.
     * The caller can still read the contents after this time elapses but will incur a new round-trip request/response.
     */
    private static final Duration BLOB_CONNECTION_CLOSED_TIMEOUT = Duration.ofSeconds(2);

    private final EmoClient _client;
    private final UriBuilder _blobStore;
    private final ScheduledExecutorService _connectionManagementService;

    public BlobStoreJersey2Client(URI endPoint, EmoClient client,
                           @Nullable ScheduledExecutorService connectionManagementService) {
        _client = checkNotNull(client, "client");
        _blobStore = EmoUriBuilder.fromUri(endPoint);

        if (connectionManagementService != null) {
            _connectionManagementService = connectionManagementService;
        } else {
            // Create a default single-threaded executor service
            _connectionManagementService = Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder().setNameFormat("blob-store-client-connection-reaper-%d").build());
        }
    }

    @Override
    public Iterator<Table> listTables(String apiKey, @Nullable String fromTableExclusive, long limit) {
        checkArgument(limit > 0, "Limit must be >0");
        try {
            URI uri = _blobStore.clone()
                    .segment("_table")
                    .queryParam("from", (fromTableExclusive != null) ? new Object[]{fromTableExclusive} : new Object[0])
                    .queryParam("limit", limit)
                    .build();
            return _client.resource(uri)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .get(new TypeReference<Iterator<Table>>(){});
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public void createTable(String apiKey, String table, TableOptions options, Map<String, String> attributes, Audit audit)
            throws TableExistsException {
        checkNotNull(table, "table");
        checkNotNull(options, "options");
        checkNotNull(attributes, "attributes");
        checkNotNull(audit, "audit");
        URI uri = _blobStore.clone()
                .segment("_table", table)
                .queryParam("options", RisonHelper.asORison(options))
                .queryParam("audit", RisonHelper.asORison(audit))
                .build();
        try {
            _client.resource(uri)
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .put(attributes);
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public void dropTable(String apiKey, String table, Audit audit) throws UnknownTableException {
        checkNotNull(table, "table");
        checkNotNull(audit, "audit");
        URI uri = _blobStore.clone()
                .segment("_table", table)
                .queryParam("audit", RisonHelper.asORison(audit))
                .build();
        try {
            _client.resource(uri)
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .delete();
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public void purgeTableUnsafe(String apiKey, String table, Audit audit) {
        checkNotNull(table, "table");
        checkNotNull(audit, "audit");
        URI uri = _blobStore.clone()
                .segment("_table", table, "purge")
                .queryParam("audit", RisonHelper.asORison(audit))
                .build();
        try {
            _client.resource(uri)
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .post();
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public boolean getTableExists(String apiKey, String table) {
        checkNotNull(table, "table");
        URI uri = _blobStore.clone()
                .segment("_table", table)
                .build();
        EmoResponse response = _client.resource(uri)
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                .head();
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return true;
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode() &&
                UnknownTableException.class.getName().equals(response.getFirstHeader("X-BV-Exception"))) {
            return false;
        } else {
            throw convertException(new EmoClientException(response));
        }
    }

    @Override
    public boolean isTableAvailable(String apiKey, String table) {
        checkNotNull(table, "table");
        return getTableMetadata(apiKey, table).getAvailability() != null;
    }

    @Override
    public Table getTableMetadata(String apiKey, String table) {
        checkNotNull(table, "table");
        try {
            URI uri = _blobStore.clone()
                    .segment("_table", table, "metadata")
                    .build();
            return _client.resource(uri)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .get(Table.class);
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public Map<String, String> getTableAttributes(String apiKey, String table) throws UnknownTableException {
        checkNotNull(table, "table");
        try {
            URI uri = _blobStore.clone()
                    .segment("_table", table)
                    .build();
            return _client.resource(uri)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .get(new TypeReference<Map<String, String>>(){});
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public void setTableAttributes(String apiKey, String table, Map<String, String> attributes, Audit audit) {
        checkNotNull(table, "table");
        checkNotNull(attributes, "attributes");
        checkNotNull(audit, "audit");
        URI uri = _blobStore.clone()
                .segment("_table", table, "attributes")
                .queryParam("audit", RisonHelper.asORison(audit))
                .build();
        try {
            _client.resource(uri)
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .put(attributes);
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public TableOptions getTableOptions(String apiKey, String table) throws UnknownTableException {
        checkNotNull(table, "table");
        try {
            URI uri = _blobStore.clone()
                    .segment("_table", table, "options")
                    .build();
            return _client.resource(uri)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .get(TableOptions.class);
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public long getTableApproximateSize(String apiKey, String table) {
        checkNotNull(table, "table");
        try {
            URI uri = _blobStore.clone()
                    .segment("_table", table, "size")
                    .build();
            return _client.resource(uri)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .get(Long.class);
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public BlobMetadata getMetadata(String apiKey, String table, String blobId) throws BlobNotFoundException {
        checkNotNull(table, "table");
        checkNotNull(blobId, "blobId");
        try {
            EmoResponse response = _client.resource(toUri(table, blobId))
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .head();
            if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode() &&
                    BlobNotFoundException.class.getName().equals(response.getFirstHeader("X-BV-Exception"))) {
                throw new BlobNotFoundException(blobId, new EmoClientException(response));

            } else if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                throw new EmoClientException(response);

            }
            return parseMetadataHeaders(blobId, response);
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public Iterator<BlobMetadata> scanMetadata(String apiKey, String table, @Nullable String fromBlobIdExclusive, long limit) {
        checkNotNull(table, "table");
        checkArgument(limit > 0, "Limit must be >0");
        try {
            URI uri = _blobStore.clone()
                    .segment(table)
                    .queryParam("from", (fromBlobIdExclusive != null) ? new Object[]{fromBlobIdExclusive} : new Object[0])
                    .queryParam("limit", limit)
                    .build();
            return _client.resource(uri)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .get(new TypeReference<Iterator<BlobMetadata>>(){});
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public Blob get(String apiKey, String table, String blobId) throws BlobNotFoundException {
        return get(apiKey, table, blobId, null);
    }

    @Override
    public Blob get(String apiKey, String table, String blobId, @Nullable RangeSpecification rangeSpec)
            throws BlobNotFoundException, RangeNotSatisfiableException {
        BlobRequest request = new BlobRequest(apiKey, table, blobId, rangeSpec);
        BlobResponse response = get(request);

        // Since we cannot guarantee if/when the caller will close the blob input stream schedule the stream
        // to be closed shortly in the future.  If the caller tries to read the stream after this happens
        // it will force a new round-trip to the server.

        // Create a weak reference so that if the returned blob is fully dereferenced then the response can be
        // finalized (which will ensure the stream is closed) and garbage collected without the scheduled runnable
        // holding the only remaining reference.
        final WeakReference<BlobResponse> weakResponse = new WeakReference<>(response);

        assert _connectionManagementService != null;
        _connectionManagementService.schedule(
                new Runnable() {
                    @Override
                    public void run() {
                        BlobResponse response = weakResponse.get();
                        if (response != null) {
                            response.ensureInputStreamClosed();
                        }
                    }
                },
                BLOB_CONNECTION_CLOSED_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        return new DefaultBlob(response.getMetadata(), response.getRange(), streamSupplier(request, response));
    }

    private BlobResponse get(BlobRequest blobRequest)
            throws BlobNotFoundException, RangeNotSatisfiableException {
        checkNotNull(blobRequest, "blobRequest");
        String table = checkNotNull(blobRequest.getTable(), "table");
        String blobId = checkNotNull(blobRequest.getBlobId(), "blobId");
        RangeSpecification rangeSpec = blobRequest.getRangeSpecification();
        String apiKey = blobRequest.getApiKey();

        try {
            EmoResource request = _client.resource(toUri(table, blobId));
            if (rangeSpec != null) {
                request.header(HttpHeaders.RANGE, rangeSpec);
            }
            EmoResponse response = request
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .get(EmoResponse.class);

            int status = response.getStatus();
            if (status != Response.Status.OK.getStatusCode() && status != HTTP_PARTIAL_CONTENT) {
                throw new EmoClientException(response);
            }

            BlobMetadata metadata = parseMetadataHeaders(blobId, response);
            InputStream input = response.getEntityInputStream();
            boolean rangeApplied = true;

            // Parse range-related data.
            Range range;
            String contentRange = response.getFirstHeader(HttpHeaders.CONTENT_RANGE);
            if (status == Response.Status.OK.getStatusCode()) {
                checkState(contentRange == null, "Unexpected HTTP 200 response with Content-Range header.");
                if (rangeSpec == null) {
                    // Normal GET request without a Range header
                    range = new Range(0, metadata.getLength());
                } else {
                    // Server ignored the Range header.  Maybe a proxy stripped it out?
                    range = rangeSpec.getRange(metadata.getLength());
                    rangeApplied = false;
                }
            } else if (status == HTTP_PARTIAL_CONTENT) {
                // Normal GET request with a Range header and a 206 Partial Content response
                checkState(rangeSpec != null, "Unexpected HTTP 206 response to request w/out a Range header.");
                checkState(contentRange != null, "Unexpected HTTP 206 response w/out Content-Range header.");
                range = parseContentRange(contentRange);
            } else {
                throw new IllegalStateException();  // Shouldn't get here
            }

            return new BlobResponse(metadata, range, rangeApplied, input);
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    /**
     * Parses an HTTP "Content-Range" header in an HTTP 206 Partial Content response.
     */
    private Range parseContentRange(String contentRange) {
        Matcher matcher = CONTENT_RANGE_PATTERN.matcher(contentRange);
        checkState(matcher.matches(), "Unexpected Content-Range header: %s", contentRange);
        long start = Long.parseLong(matcher.group(1));
        long end = Long.parseLong(matcher.group(2));  // Inclusive
        return new Range(start, end - start + 1);
    }

    /**
     * Parses HTTP headers into a {@link BlobMetadata} object.
     */
    private BlobMetadata parseMetadataHeaders(String blobId, EmoResponse response) {
        // The server always sets X-BV-Length.  It's similar to Content-Length but proxies etc. shouldn't mess with it.
        String lengthString = response.getFirstHeader(X_BV_PREFIX + "Length");
        checkState(lengthString != null, "BlobStore request is missing expected required X-BV-Length header.");
        long length = Long.parseLong(lengthString);

        // Extract signature hash values.
        String md5 = base64ToHex(response.getFirstHeader(HttpHeaders.CONTENT_MD5));
        String sha1 = stripQuotes(response.getFirstHeader(HttpHeaders.ETAG));

        // Extract attribute map specified when the blob was first uploaded.
        Map<String, String> attributes = Maps.newHashMap();
        for (Map.Entry<String, List<String>> entry : response.getHeaders()) {
            if (entry.getKey().startsWith(X_BVA_PREFIX)) {
                attributes.put(entry.getKey().substring(X_BVA_PREFIX.length()), entry.getValue().get(0));
            }
        }

        return new DefaultBlobMetadata(blobId, response.getLastModified(), length, md5, sha1, attributes);
    }

    private StreamSupplier streamSupplier(final BlobRequest request, final BlobResponse response) {
        return out -> {
            InputStream in = response.getInputStream();
            if (in == null) {
                // The original stream has already been consumed.  Re-open a new stream from the server.
                in = get(request).getInputStream();
            }

            try {
                ByteStreams.copy(in, out);
            } finally {
                Closeables.close(in, true);
            }
        };
    }

    private String base64ToHex(String base64) {
        return (base64 != null) ? Hex.encodeHexString(Base64.decodeBase64(base64)) : null;
    }

    private String stripQuotes(String quoted) {
        return (quoted != null) ? quoted.replaceAll("\"", "") : null;
    }

    @Override
    public void put(String apiKey, String table, String blobId, Supplier<? extends InputStream> in,
                    Map<String, String> attributes)
            throws IOException {
        checkNotNull(table, "table");
        checkNotNull(blobId, "blobId");
        checkNotNull(in, "in");
        checkNotNull(attributes, "attributes");
        try {
            // Encode the ttl as a URL query parameter
            URI uri = _blobStore.clone()
                    .segment(table, blobId)
                    .build();
            // Encode the rest of the attributes as request headers
            EmoResource request = _client.resource(uri);
            for (Map.Entry<String, String> entry : attributes.entrySet()) {
                request.header(X_BVA_PREFIX + entry.getKey(), entry.getValue());
            }
            // Upload the object
            request.type(MediaType.APPLICATION_OCTET_STREAM_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .put(in.get());
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public void delete(String apiKey, String table, String blobId) {
        checkNotNull(table, "table");
        checkNotNull(blobId, "blobId");
        try {
            _client.resource(toUri(table, blobId))
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .delete();
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public Collection<String> getTablePlacements(String apiKey) {
        try {
            URI uri = _blobStore.clone()
                    .segment("_tableplacement")
                    .build();
            return _client.resource(uri)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .get(new TypeReference<List<String>>(){});
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    private URI toUri(String table, String blobId) {
        return _blobStore.clone().segment(table, blobId).build();
    }

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private RuntimeException convertException(EmoClientException e) {
        EmoResponse response = e.getResponse();
        String exceptionType = response.getFirstHeader("X-BV-Exception");

        if (response.getStatus() == Response.Status.BAD_REQUEST.getStatusCode() &&
                IllegalArgumentException.class.getName().equals(exceptionType)) {
            return new IllegalArgumentException(response.getEntity(String.class), e);

        } else if (response.getStatus() == Response.Status.CONFLICT.getStatusCode() &&
                TableExistsException.class.getName().equals(exceptionType)) {
            if (response.hasEntity()) {
                return (RuntimeException) response.getEntity(TableExistsException.class).initCause(e);
            } else {
                return (RuntimeException) new TableExistsException().initCause(e);
            }

        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode() &&
                UnknownTableException.class.getName().equals(exceptionType)) {
            if (response.hasEntity()) {
                return (RuntimeException) response.getEntity(UnknownTableException.class).initCause(e);
            } else {
                return (RuntimeException) new UnknownTableException().initCause(e);
            }

        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode() &&
                BlobNotFoundException.class.getName().equals(exceptionType)) {
            if (response.hasEntity()) {
                return (RuntimeException) response.getEntity(BlobNotFoundException.class).initCause(e);
            } else {
                return (RuntimeException) new BlobNotFoundException().initCause(e);
            }
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode() &&
                UnknownPlacementException.class.getName().equals(exceptionType)) {
            if (response.hasEntity()) {
                return (RuntimeException) response.getEntity(UnknownPlacementException.class).initCause(e);
            } else {
                return (RuntimeException) new UnknownPlacementException().initCause(e);
            }

        } else if (response.getStatus() == 416 /* REQUESTED_RANGE_NOT_SATIFIABLE */ &&
                RangeNotSatisfiableException.class.getName().equals(exceptionType)) {
            if (response.hasEntity()) {
                return (RuntimeException) response.getEntity(RangeNotSatisfiableException.class).initCause(e);
            } else {
                return (RuntimeException) new RangeNotSatisfiableException(null, -1, -1).initCause(e);
            }

        } else if (response.getStatus() == Response.Status.MOVED_PERMANENTLY.getStatusCode() &&
                UnsupportedOperationException.class.getName().equals(exceptionType)) {
            return new UnsupportedOperationException("Permanent redirect: " + response.getLocation(), e);

        } else if (response.getStatus() == Response.Status.FORBIDDEN.getStatusCode() &&
                UnauthorizedException.class.getName().equals(exceptionType)) {
            if (response.hasEntity()) {
                return (RuntimeException) response.getEntity(UnauthorizedException.class).initCause(e);
            } else {
                return (RuntimeException) new UnauthorizedException().initCause(e);
            }
        } else if (response.getStatus() == Response.Status.SERVICE_UNAVAILABLE.getStatusCode() &&
                ServiceUnavailableException.class.getName().equals(exceptionType)) {
            if (response.hasEntity()) {
                return (RuntimeException) response.getEntity(ServiceUnavailableException.class).initCause(e);
            } else {
                return (RuntimeException) new ServiceUnavailableException().initCause(e);
            }
        }

        return e;
    }

    /**
     * Helper object to encapsulate the parameters for a read blob request.
     */
    private static class BlobRequest {
        final String _apiKey;
        final String _table;
        final String _blobId;
        @Nullable
        final RangeSpecification _rangeSpec;

        private BlobRequest(String apiKey, String table, String blobId, RangeSpecification rangeSpec) {
            _apiKey = apiKey;
            _table = table;
            _blobId = blobId;
            _rangeSpec = rangeSpec;
        }

        private String getApiKey() {
            return _apiKey;
        }

        private String getTable() {
            return _table;
        }

        private String getBlobId() {
            return _blobId;
        }

        @Nullable
        private RangeSpecification getRangeSpecification() {
            return _rangeSpec;
        }
    }

    /**
     * Helper object to encapsulate the response for a read blob request and provide guaranteed closure for the
     * underlying resources.
     */
    private static class BlobResponse {
        final private BlobMetadata _metadata;
        final private Range _range;
        final private boolean _rangeApplied;
        final private InputStream _stream;
        private final AtomicBoolean _inputStreamConsumed = new AtomicBoolean(false);

        private BlobResponse(BlobMetadata metadata, Range range, boolean rangeApplied, InputStream stream) {
            _metadata = metadata;
            _range = range;
            _rangeApplied = rangeApplied;
            _stream = stream;
        }

        private BlobMetadata getMetadata() {
            return _metadata;
        }

        private Range getRange() {
            return _range;
        }

        private boolean claimInputStream() {
            return _inputStreamConsumed.compareAndSet(false, true);
        }

        @Nullable
        public InputStream getInputStream() throws IOException {
            if (!claimInputStream()) {
                // The input stream has already been consumed
                return null;
            }

            if (_rangeApplied) {
                // Either the entire blob was requested or the requested range was applied by the server
                return _stream;
            }

            // The client requested a sub-range of the blob but the server ignored it.  Manipulate the input stream
            // to return only the client requested range.
            ByteStreams.skipFully(_stream, _range.getOffset());
            return ByteStreams.limit(_stream, _range.getLength());
        }

        public void ensureInputStreamClosed() {
            // Only close the stream if no caller ever attempted to read it.
            if (claimInputStream()) {
                try {
                    Closeables.close(_stream, true);
                } catch (IOException e) {
                    // Already caught and logged
                }
            }
        }

        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            ensureInputStreamClosed();
        }
    }
}
