package test.client.commons.utils;

import com.bazaarvoice.emodb.common.api.UnauthorizedException;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.TableExistsException;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.Update;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static test.client.commons.utils.Names.uniqueName;
import static test.client.commons.utils.TableUtils.getAudit;

public class DataStoreHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataStoreHelper.class);

    private String documentKey;

    private String tableName;

    private Map<String, String> template;

    private final String apiTested;

    private final String placement;

    private String clientName = "gatekeeper_datastore_client";

    private final String runID;

    private final DataStore dataStore;

    private String subscriptionName;

    private void generateResources() {
        tableName = uniqueName(documentKey, apiTested, clientName, runID);
        template = TableUtils.getTemplate(apiTested, clientName, runID);
    }

    public DataStoreHelper setClientName(String clientName) {
        this.clientName = clientName;
        this.generateResources();
        return this;
    }

    public DataStoreHelper(DataStore dataStore, String placement, String documentKey, String apiTested, String runID) {
        this.documentKey = documentKey;
        this.apiTested = apiTested;
        this.dataStore = dataStore;
        this.placement = placement;
        this.runID = runID;
        this.generateResources();
    }

    public String createTable() {
        createDataTable(dataStore, tableName, template, placement);
        return tableName;
    }

    public List<Update> updateDocumentMultipleTimes(int iterations, int startAt) {
        return updateDocumentMultipleTimes(dataStore, tableName, documentKey, iterations, startAt);
    }

    public List<Update> generateUpdatesList(int totalDocuments) {
        return generateUpdatesList(totalDocuments, tableName, documentKey);
    }

    public DataStoreHelper setSubscriptionName(String extra) {
        subscriptionName = String.format("gk_%s_%s_sub_%s", this.apiTested, this.documentKey, extra);
        return this;
    }

    public String getDocumentKey() {
        return this.documentKey;
    }

    public String getTableName() {
        return this.tableName;
    }

    public Map<String, String> getTemplate() {
        return this.template;
    }

    public String getSubscriptionName() {
        return this.subscriptionName;
    }

    public void setDocumentKey(String documentKey) {
        this.documentKey = documentKey;
    }

    private static List<Update> updateDocumentMultipleTimes(DataStore dataStore, String tableName, String key, int iterations, int startAt) {
        String updateString = "iteration";
        List<Update> listToReturn = new ArrayList<>();
        for (int i = 0; i < iterations; i++) {
            listToReturn.add(updateDocument(
                    dataStore,
                    tableName,
                    key,
                    Deltas.fromString("{..,\"" + updateString + "\":" + (startAt + i) + "}")));
        }

        return listToReturn;
    }

    public static void createDataTable(DataStore dataStore, String tableName, Map<String, String> template, String placement) {
        LOGGER.debug("Creating table {}, placement: {}", tableName, placement);

        TableOptions options = new TableOptionsBuilder().setPlacement(placement).build();
        try {
            dataStore.createTable(tableName, options, template, getAudit(String.format("Creating table: %s", tableName)));
        } catch (TableExistsException te) {
            throw new com.bazaarvoice.emodb.sor.api.TableExistsException(String.format("Cannot create table that already exists: %s", tableName));
        } catch (UnauthorizedException e) {
            throw new UnauthorizedException(e.getMessage());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (!dataStore.getTableExists(tableName)) {
            throw new IllegalStateException(String.format("Table '%s' was not created!", tableName));
        }

        LOGGER.debug("Created table {}", tableName);
    }

    public static List<Update> generateUpdatesList(int amount, String tableName, String documentPrefix) {
        return generateUpdatesList(amount, tableName, documentPrefix, null);
    }

    public static List<Update> generateUpdatesList(int amount, String tableName, String documentPrefix, UUID timeUUID) {
        List<Update> updateList = new ArrayList<>();

        for (int i = 1; i <= amount; i++) {
            String key = String.format("%s-%s", documentPrefix, i);
            Map<String, Object> delta = ImmutableMap.<String, Object>builder()
                    .put("iteration", i)
                    .build();

            timeUUID = timeUUID == null ? TimeUUIDs.newUUID() : timeUUID;

            LOGGER.debug("Adding {} for table {} with UUID {}", key, tableName, timeUUID);
            updateList.add(new Update(tableName, key, timeUUID, Deltas.literal(delta),
                    getAudit(String.format("Content for %s/%s", tableName, key))));
        }
        return updateList;
    }

    public static Update updateDocument(DataStore dataStore, String tableName, String key, Delta delta) {
        return updateDocument(dataStore, tableName, key, delta, 0, null);
    }

    public static Update updateDocument(DataStore dataStore, String tableName, String key, Delta delta, long minsBehindNow, UUID uuid) {
        if (uuid == null) {
            uuid = TimeUUIDs.uuidForTimeMillis(System.currentTimeMillis() - Duration.ofSeconds(minsBehindNow).toMillis());
        }
        Update update = new Update(tableName, key, uuid, delta, getAudit(String.format("update_%s/%s", tableName, key)));
        LOGGER.debug("Updating table {} with update {}", tableName, update);
        dataStore.update(update.getTable(), update.getKey(), update.getChangeId(), update.getDelta(), update.getAudit());
        return update;
    }

    public static List<Change> getTimelineChangeList(DataStore dataStore, String tableName, String key,
                                                     boolean includeContent, boolean reversed) {
        return Lists.newArrayList(dataStore.getTimeline(tableName, key, includeContent, false,
                null, null, reversed, 100, ReadConsistency.STRONG));
    }

    /**
     * Compact and return the latest compaction in the timeline. Note that the returned compaction may not be the result
     * of the compact, as it can result in no compactions.
     */
    public static Change compactdata(DataStore dataStore, String tableName, String key, Duration ttlOverride) {
        try {
            dataStore.compact(tableName, key, ttlOverride, ReadConsistency.STRONG, WriteConsistency.STRONG);
        } catch (Exception ex) {
            LOGGER.error(String.format("caught '%s (%s)' calling compact() - Should work as of v0.64",
                    ex.getClass().getName(), ex.getMessage()));
        }

        List<Change> newCompactions = getTimelineChangeList(dataStore, tableName, key, true, true)
                .stream().filter(doc -> doc.getCompaction() != null).collect(Collectors.toList());

        return newCompactions.isEmpty() ? null : newCompactions.get(0);
    }
}
