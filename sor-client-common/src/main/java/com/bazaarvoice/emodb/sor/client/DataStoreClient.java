package com.bazaarvoice.emodb.sor.client;

import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.bazaarvoice.emodb.auth.proxy.Credential;
import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.client.EmoClientException;
import com.bazaarvoice.emodb.client.EmoResponse;
import com.bazaarvoice.emodb.client.uri.EmoUriBuilder;
import com.bazaarvoice.emodb.common.api.ServiceUnavailableException;
import com.bazaarvoice.emodb.common.api.Ttls;
import com.bazaarvoice.emodb.common.api.UnauthorizedException;
import com.bazaarvoice.emodb.common.json.JsonStreamProcessingException;
import com.bazaarvoice.emodb.common.json.RisonHelper;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.AuditSizeLimitException;
import com.bazaarvoice.emodb.sor.api.AuthDataStore;
import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.Coordinate;
import com.bazaarvoice.emodb.sor.api.DeltaSizeLimitException;
import com.bazaarvoice.emodb.sor.api.FacadeOptions;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.StashNotAvailableException;
import com.bazaarvoice.emodb.sor.api.Table;
import com.bazaarvoice.emodb.sor.api.TableExistsException;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.UnknownPlacementException;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.sor.api.UnpublishedDatabusEvent;
import com.bazaarvoice.emodb.sor.api.Update;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.PeekingIterator;
import org.apache.commons.codec.binary.Base64;

import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Data store client implementation that routes System of Record API calls to the EmoDB service.  The actual HTTP
 * communication is managed by the {@link EmoClient} implementation to allow for flexible usage by variety of HTTP client
 * implementations, such as Jersey.
 */
public class DataStoreClient implements AuthDataStore {

    /** Must match the service name in the EmoService class. */
    /*package*/ static final String BASE_SERVICE_NAME = "emodb-sor-1";

    /** Must match the @Path annotation on the DataStoreResource class. */
    public static final String SERVICE_PATH = "/sor/1";

    private static final MediaType APPLICATION_X_JSON_DELTA_TYPE = new MediaType("application", "x.json-delta");

    private static final Duration UPDATE_ALL_REQUEST_DURATION = Duration.ofSeconds(1);

    private final EmoClient _client;
    private final UriBuilder _dataStore;

    public DataStoreClient(URI endPoint, EmoClient client) {
        _client = checkNotNull(client, "client");
        _dataStore = EmoUriBuilder.fromUri(endPoint);
    }

    @Override
    public Iterator<Table> listTables(String apiKey, @Nullable String fromTableExclusive, long limit) {
        checkArgument(limit > 0, "Limit must be >0");
        try {
            URI uri = _dataStore.clone()
                    .segment("_table")
                    .queryParam("from", optional(fromTableExclusive))
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
    public Iterator<UnpublishedDatabusEvent> listUnpublishedDatabusEvents(String apiKey, @Nullable Date fromInclusive, @Nullable Date toExclusive) {
        try {
            URI uri = _dataStore.clone()
                    .segment("_unpublishedevents")
                    .queryParam("from", optional(fromInclusive))
                    .queryParam("to", optional(toExclusive))
                    .build();
            return _client.resource(uri)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .get(new TypeReference<Iterator<UnpublishedDatabusEvent>>(){});
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public void createTable(String apiKey, String table, TableOptions options, Map<String, ?> template, Audit audit) throws TableExistsException {
        checkNotNull(table, "table");
        checkNotNull(options, "options");
        checkNotNull(template, "template");
        checkNotNull(audit, "audit");
        URI uri = _dataStore.clone()
                .segment("_table", table)
                .queryParam("options", RisonHelper.asORison(options))
                .queryParam("audit", RisonHelper.asORison(audit))
                .build();
        for (int attempt = 0; ; attempt++) {
            try {
                _client.resource(uri)
                        .type(MediaType.APPLICATION_JSON_TYPE)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .put(template);
                return;
            } catch (EmoClientException e) {
                // The SoR returns a 301 response when we need to make this request against a different data center.
                // Follow the redirect a few times but don't loop forever.
                if (e.getResponse().getStatus() == Response.Status.MOVED_PERMANENTLY.getStatusCode() && attempt < 5) {
                    uri = e.getResponse().getLocation();
                    continue;
                }
                throw convertException(e);
            }
        }
    }

    @Override
    public void dropTable(String apiKey, String table, Audit audit) throws UnknownTableException {
        checkNotNull(table, "table");
        checkNotNull(audit, "audit");
        URI uri = _dataStore.clone()
                .segment("_table", table)
                .build();
        EmoResponse response = _client.resource(uri)
                .queryParam("audit", RisonHelper.asORison(audit))
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                .delete(EmoResponse.class);
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw convertException(new EmoClientException(response));
        }
    }

    @Override
    public void purgeTableUnsafe(String apiKey, String table, Audit audit) {
        throw new UnsupportedOperationException("Purging a table requires administrator privileges.");
    }

    @Override
    public boolean getTableExists(String apiKey, String table) {
        checkNotNull(table, "table");
        URI uri = _dataStore.clone()
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

    public boolean isTableAvailable(String apiKey, String table) {
        checkNotNull(table, "table");
        return getTableMetadata(apiKey, table).getAvailability() != null;
    }

    @Override
    public Table getTableMetadata(String apiKey, String table) {
        checkNotNull(table, "table");
        try {
            URI uri = _dataStore.clone()
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
    public Map<String, Object> getTableTemplate(String apiKey, String table) {
        checkNotNull(table, "table");
        try {
            URI uri = _dataStore.clone()
                    .segment("_table", table)
                    .build();
            return _client.resource(uri)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .get(new TypeReference<Map<String,Object>>(){});
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public void setTableTemplate(String apiKey, String table, Map<String, ?> template, Audit audit) {
        checkNotNull(table, "table");
        checkNotNull(template, "template");
        checkNotNull(audit, "audit");
        URI uri = _dataStore.clone()
                .segment("_table", table, "template")
                .queryParam("audit", RisonHelper.asORison(audit))
                .build();
        for (int attempt = 0; ; attempt++) {
            try {
                _client.resource(uri)
                        .type(MediaType.APPLICATION_JSON_TYPE)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .put(template);
                return;
            } catch (EmoClientException e) {
                // The SoR returns a 301 response when we need to make this request against a different data center.
                // Follow the redirect a few times but don't loop forever.
                if (e.getResponse().getStatus() == Response.Status.MOVED_PERMANENTLY.getStatusCode() && attempt < 5) {
                    uri = e.getResponse().getLocation();
                    continue;
                }
                throw convertException(e);
            }
        }
    }

    @Override
    public TableOptions getTableOptions(String apiKey, String table) {
        checkNotNull(table, "table");
        try {
            URI uri = _dataStore.clone()
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
            URI uri = _dataStore.clone()
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
    public long getTableApproximateSize(String apiKey, String table, int limit) throws UnknownTableException {
        checkNotNull(table, "table");
        checkNotNull(limit);
        checkArgument(limit > 0, "limit must be greater than 0");
        try {
            URI uri = _dataStore.clone()
                    .segment("_table", table, "size")
                    .queryParam("limit", limit)
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
    public Map<String, Object> get(String apiKey, String table, String key) {
        return get(apiKey, table, key, ReadConsistency.STRONG);
    }

    @Override
    public Map<String, Object> get(String apiKey, String table, String key, ReadConsistency consistency) {
        checkNotNull(table, "table");
        checkNotNull(key, "key");
        checkNotNull(consistency, "consistency");
        try {
            URI uri = _dataStore.clone()
                    .segment(table, key)
                    .queryParam("consistency", consistency)
                    .build();
            return _client.resource(uri)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .get(new TypeReference<Map<String,Object>>(){});
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public Iterator<Change> getTimeline(String apiKey, String table, String key, boolean includeContentData, boolean includeAuditInformation,
                                        @Nullable UUID start, @Nullable UUID end, boolean reversed, long limit, ReadConsistency consistency) {
        checkNotNull(table, "table");
        checkNotNull(key, "key");
        if (start != null && end != null) {
            if (reversed) {
                checkArgument(TimeUUIDs.compare(start, end) >= 0, "Start must be >=End for reversed ranges");
            } else {
                checkArgument(TimeUUIDs.compare(start, end) <= 0, "Start must be <=End");
            }
        }
        checkArgument(limit > 0, "Limit must be >0");
        checkNotNull(consistency, "consistency");
        try {
            URI uri = _dataStore.clone()
                    .segment(table, key, "timeline")
                    .queryParam("data", includeContentData)
                    .queryParam("audit", includeAuditInformation)
                    .queryParam("start", optional(start))
                    .queryParam("end", optional(end))
                    .queryParam("reversed", reversed)
                    .queryParam("limit", limit)
                    .queryParam("consistency", consistency)
                    .build();
            return _client.resource(uri)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .get(new TypeReference<Iterator<Change>>(){});
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public Iterator<Map<String, Object>> scan(String apiKey, String table, @Nullable String fromKeyExclusive,
                                              long limit, boolean includeDeletes, ReadConsistency consistency) {
        checkNotNull(table, "table");
        checkArgument(limit > 0, "Limit must be >0");
        checkNotNull(consistency, "consistency");
        try {
            URI uri = _dataStore.clone()
                    .segment(table)
                    .queryParam("from", optional(fromKeyExclusive))
                    .queryParam("limit", limit)
                    .queryParam("includeDeletes", includeDeletes)
                    .queryParam("consistency", consistency)
                    .build();
            return _client.resource(uri)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .get(new TypeReference<Iterator<Map<String,Object>>>(){});
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public Collection<String> getSplits(String apiKey, String table, int desiredRecordsPerSplit) {
        checkNotNull(table, "table");
        checkArgument(desiredRecordsPerSplit > 0, "DesiredRecordsPerSplit must be >0");
        try {
            URI uri = _dataStore.clone()
                    .segment("_split", table)
                    .queryParam("size", desiredRecordsPerSplit)
                    .build();
            return _client.resource(uri)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .get(new TypeReference<List<String>>(){});
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public Iterator<Map<String, Object>> getSplit(String apiKey, String table, String split, @Nullable String fromKeyExclusive,
                                                  long limit, boolean includeDeletes, ReadConsistency consistency) {
        checkNotNull(table, "table");
        checkNotNull(split, "split");
        checkArgument(limit > 0, "Limit must be >0");
        checkNotNull(consistency, "consistency");
        try {
            URI uri = _dataStore.clone()
                    .segment("_split", table, split)
                    .queryParam("from", optional(fromKeyExclusive))
                    .queryParam("limit", limit)
                    .queryParam("includeDeletes", includeDeletes)
                    .queryParam("consistency", consistency)
                    .build();
            return _client.resource(uri)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .get(new TypeReference<Iterator<Map<String,Object>>>(){});
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public Iterator<Map<String, Object>> multiGet(String apiKey, List<Coordinate> coordinates) {
        return multiGet(apiKey, coordinates, ReadConsistency.STRONG);
    }

    @Override
    public Iterator<Map<String, Object>> multiGet(String apiKey, final List<Coordinate> coordinates, ReadConsistency consistency) {
        checkNotNull(coordinates, "coordinates");
        checkNotNull(consistency, "consistency");
        try {
            UriBuilder uriBuilder = _dataStore.clone().segment("_multiget").queryParam("consistency", consistency);
            for(Coordinate coordinate : coordinates) {
                uriBuilder.queryParam("id", coordinate.toString());
            }
            URI uri = uriBuilder.build();
            return _client.resource(uri)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .get(new TypeReference<Iterator<Map<String,Object>>>(){});
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public void update(String apiKey, String table, String key, UUID changeId, Delta delta, Audit audit) {
        update(apiKey, table, key, changeId, delta, audit, WriteConsistency.STRONG);
    }

    @Override
    public void update(String apiKey, String table, String key, UUID changeId, Delta delta, Audit audit, WriteConsistency consistency) {
        update(apiKey, table, key, changeId, delta, audit, consistency, false, ImmutableSet.<String>of());
    }

    private void update(String apiKey, String table, String key, UUID changeId, Delta delta, Audit audit, WriteConsistency consistency,
                        boolean facade, Set<String> tags) {
        checkNotNull(table, "table");
        checkNotNull(key, "key");
        checkNotNull(delta, "delta");
        checkNotNull(audit, "audit");
        checkNotNull(consistency, "consistency");
        try {
            UriBuilder uriBuilder = _dataStore.clone()
                    .segment(facade ? "_facade" : "", table, key)
                    .queryParam("changeId", (changeId != null) ? changeId : TimeUUIDs.newUUID())
                    .queryParam("audit", RisonHelper.asORison(audit))
                    .queryParam("consistency", consistency);
            for(String tag : tags) {
                uriBuilder.queryParam("tag", tag);
            }
            _client.resource(uriBuilder.build())
                    .type(APPLICATION_X_JSON_DELTA_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .post(delta.toString());
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public void updateAll(String apiKey, Iterable<Update> updates) {
        updateAll(apiKey, updates, false, ImmutableSet.<String>of());
    }

    @Override
    public void updateAll(String apiKey, Iterable<Update> updates, Set<String> tags) {
        updateAll(apiKey, updates, false, tags);
    }

    private void updateAll(String apiKey, Iterable<Update> updates, boolean facade, Set<String> tags) {
        // This method takes an Iterable instead of an Iterator so it can be retried (by Ostrich etc.) if it fails.

        // If just one update, use the slightly more compact single record REST api.
        if (updates instanceof Collection && ((Collection) updates).size() == 1) {
            Update update = Iterables.getOnlyElement(updates);
            update(apiKey, update.getTable(), update.getKey(), update.getChangeId(), update.getDelta(), update.getAudit(),
                    update.getConsistency(), facade, tags);
            return;
        }

        // Otherwise, use the streaming API to send multiple updates per HTTP request.  Break the updates into batches
        // such that this makes approximately one HTTP request per second.  The goal is to make requests big enough to
        // get the performance benefits of batching while being small enough that they show up with regularity in the
        // request logs--don't want an hour long POST that doesn't show up in the request log until the end of the hour.
        Iterator<Update> updatesIter = updates.iterator();
        for (long batchIdx = 0; updatesIter.hasNext(); batchIdx++) {
            PeekingIterator<Update> batchIter = TimeLimitedIterator.create(updatesIter, UPDATE_ALL_REQUEST_DURATION, 1);

            // Grab the first update, assume it's representative (but note it may not be) and copy some of its
            // attributes into the URL query parameters for the *sole* purpose of making the server request logs easier
            // to read.  The server ignores the query parameters--only the body of the POST actually matters.
            Update first = batchIter.peek();
            try {
                UriBuilder uriBuilder = _dataStore.clone()
                        .segment(facade ? "_facade" : "", "_stream")
                        .queryParam("batch", batchIdx)
                        .queryParam("table", first.getTable())
                        .queryParam("key", first.getKey())
                        .queryParam("audit", RisonHelper.asORison(first.getAudit()))
                        .queryParam("consistency", first.getConsistency());
                for(String tag : tags) {
                    uriBuilder.queryParam("tag", tag);
                }
                _client.resource(uriBuilder.build())
                        .type(MediaType.APPLICATION_JSON_TYPE)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .post(batchIter);
            } catch (EmoClientException e) {
                throw convertException(e);
            }
        }
    }

    @Override
    public void createFacade(String apiKey, String table, FacadeOptions options, Audit audit)
            throws TableExistsException {
        checkNotNull(table, "table");
        checkNotNull(options, "options");
        checkNotNull(audit, "audit");
        URI uri = _dataStore.clone()
                .segment("_facade", table)
                .queryParam("options", RisonHelper.asORison(options))
                .queryParam("audit", RisonHelper.asORison(audit))
                .build();
        for (int attempt = 0; ; attempt++) {
            try {
                _client.resource(uri)
                        .type(MediaType.APPLICATION_JSON_TYPE)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .put();
                return;
            } catch (EmoClientException e) {
                // The SoR returns a 301 response when we need to make this request against a different data center.
                // Follow the redirect a few times but don't loop forever.
                if (e.getResponse().getStatus() == Response.Status.MOVED_PERMANENTLY.getStatusCode() && attempt < 5) {
                    uri = e.getResponse().getLocation();
                    continue;
                }
                throw convertException(e);
            }
        }
    }

    @Override
    public void dropFacade(String apiKey, String table, String dataCenter, Audit audit)
            throws UnknownTableException {
        throw new UnsupportedOperationException("Dropping a facade requires administrator privileges.");
    }

    @Override
    public void updateAllForFacade(String apiKey, Iterable<Update> updates) {
        updateAll(apiKey, updates, true, ImmutableSet.<String>of());
    }

    @Override
    public void updateAllForFacade(@Credential String apiKey, Iterable<Update> updates, Set<String> tags) {
        updateAll(apiKey, updates, true, tags);
    }

    @Override
    public void compact(String apiKey, String table, String key, @Nullable Duration ttlOverride, ReadConsistency readConsistency, WriteConsistency writeConsistency) {
        checkNotNull(table, "table");
        checkNotNull(key, "key");
        checkNotNull(readConsistency, "readConsistency");
        checkNotNull(writeConsistency, "writeConsistency");
        try {
            Integer ttlOverrideSeconds = (ttlOverride != null) ? Ttls.toSeconds(ttlOverride, 0, Integer.MAX_VALUE) : null;
            URI uri = _dataStore.clone()
                    .segment(table, key, "compact")
                    .queryParam("ttl", (ttlOverrideSeconds != null) ? new Object[]{ttlOverrideSeconds} : new Object[0])
                    .queryParam("readConsistency", readConsistency)
                    .queryParam("writeConsistency", writeConsistency)
                    .build();
            _client.resource(uri)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .post();
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public Collection<String> getTablePlacements(String apiKey) {
        try {
            URI uri = _dataStore.clone()
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

    @Override
    public URI getStashRoot(String apiKey)
            throws StashNotAvailableException {
        try {
            URI uri = _dataStore.clone()
                    .segment("_stashroot")
                    .build();
            String stashRoot = _client.resource(uri)
                    .accept(MediaType.TEXT_PLAIN_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .get(String.class);
            return URI.create(stashRoot);
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private RuntimeException convertException(EmoClientException e) {
        EmoResponse response = e.getResponse();
        String exceptionType = response.getFirstHeader("X-BV-Exception");

        if (response.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()) {
            if (IllegalArgumentException.class.getName().equals(exceptionType)) {
                return new IllegalArgumentException(response.getEntity(String.class), e);
            } else if (JsonStreamProcessingException.class.getName().equals(exceptionType)) {
                return new JsonStreamProcessingException(response.getEntity(String.class));
            } else if (DeltaSizeLimitException.class.getName().equals(exceptionType)) {
                return response.getEntity(DeltaSizeLimitException.class);
            } else if (AuditSizeLimitException.class.getName().equals(exceptionType)) {
                return response.getEntity(AuditSizeLimitException.class);
            }

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
                UnknownPlacementException.class.getName().equals(exceptionType)) {
            if (response.hasEntity()) {
                return (RuntimeException) response.getEntity(UnknownPlacementException.class).initCause(e);
            } else {
                return (RuntimeException) new UnknownPlacementException().initCause(e);
            }

        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode() &&
                StashNotAvailableException.class.getName().equals(exceptionType)) {
            if (response.hasEntity()) {
                return (RuntimeException) response.getEntity(StashNotAvailableException.class).initCause(e);
            } else {
                return (RuntimeException) new StashNotAvailableException().initCause(e);
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

    private String basicAuthCredentials(String credentials) {
        return String.format("Basic %s", Base64.encodeBase64String(credentials.getBytes(Charsets.UTF_8)));
    }

    private Object[] optional(Object queryArg) {
        return (queryArg != null) ? new Object[]{queryArg} : new Object[0];
    }
}
