package test.client.core;

import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.databus.api.Event;
import com.bazaarvoice.emodb.databus.api.MoveSubscriptionStatus;
import com.bazaarvoice.emodb.databus.api.PollResult;
import com.bazaarvoice.emodb.databus.api.ReplaySubscriptionStatus;
import com.bazaarvoice.emodb.databus.api.Subscription;
import com.bazaarvoice.emodb.databus.api.UnauthorizedSubscriptionException;
import com.bazaarvoice.emodb.databus.api.UnknownSubscriptionException;
import com.bazaarvoice.emodb.databus.client.DatabusClientFactory;
import com.bazaarvoice.emodb.databus.client.DatabusFixedHostDiscoverySource;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.Update;
import com.bazaarvoice.emodb.sor.client.DataStoreStreaming;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.uac.api.UserAccessControl;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.bazaarvoice.ostrich.retry.ExponentialBackoffRetry;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;
import test.client.commons.TestModuleFactory;
import test.client.commons.utils.DataStoreHelper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static test.client.commons.utils.DataStoreHelper.createDataTable;
import static test.client.commons.utils.Names.uniqueName;
import static test.client.commons.utils.RetryUtils.snooze;
import static test.client.commons.utils.TableUtils.getAudit;
import static test.client.commons.utils.TableUtils.getTemplate;
import static test.client.commons.utils.UserAccessControlUtils.createApiKey;
import static test.client.commons.utils.UserAccessControlUtils.createRole;
import static test.client.commons.utils.UserAccessControlUtils.padKey;

@Test(timeOut = 360000)
@Guice(moduleFactory = TestModuleFactory.class)
public class DatabusTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabusTest.class);
    private static final String DATABUS_TEST_CLIENT_NAME = "gatekeeper_databus_client";
    private static final AtomicInteger NEXT_API_KEY = new AtomicInteger(0);

    private Set<String> tablesToCleanupAfterTest;
    private Set<String> subscriptionsToCleanupAfterTest;

    @Inject
    @Named("placement")
    private String placement;

    @Inject
    private DataStore dataStore;

    @Inject
    private Databus databus;

    @Inject
    private UserAccessControl uac;

    @Inject
    @Named("clusterName")
    private String clusterName;

    @Inject
    @Named("emodbHost")
    private String emodbHost;

    @Inject
    @Named("runID")
    private String runID;

    @BeforeTest(alwaysRun = true)
    public void beforeTest() {
        tablesToCleanupAfterTest = new HashSet<>();
        subscriptionsToCleanupAfterTest = new HashSet<>();
    }

    @AfterTest(alwaysRun = true)
    public void afterTest() {
        tablesToCleanupAfterTest.forEach((name) -> {
            LOGGER.info("Deleting sor table: {}", name);
            try {
                dataStore.dropTable(name, getAudit("drop table " + name));
            } catch (Exception e) {
                LOGGER.warn("Error to delete sor table: " + name, e);
            }
        });

        subscriptionsToCleanupAfterTest.forEach(name -> {
            LOGGER.info("Unsubscribing {}", name);
            try {
                databus.unsubscribe(name);
            } catch (Exception e) {
                LOGGER.warn("Error to unsubscribe " + name, e);
            }
        });
    }

    @Test
    public void testGet_ignoreEtlTag() {
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "ignore_tag", "databus", runID)
                .setClientName(DATABUS_TEST_CLIENT_NAME)
                .setSubscriptionName(runID);
        final String tableName = createTable(helper);

        // By default we now add a condition for all subscriptions to skip databus events tagged with "re-etl"
        Condition condition = Conditions.and(
                Conditions.intrinsic(Intrinsic.TABLE, tableName),
                Conditions.mapBuilder().contains("runID", runID).build(),
                Conditions.not(Conditions.mapBuilder().matches("~tags", Conditions.containsAny("re-etl")).build()));

        subscribeAndCheckConditions(helper.getSubscriptionName(), condition);
    }

    private String createTable(DataStoreHelper helper) {
        final String tableName = helper.createTable();
        tablesToCleanupAfterTest.add(tableName);
        return tableName;
    }

    @Test
    public void testGet_etlTag() {
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "reetl_tag", "databus", runID)
                .setClientName(DATABUS_TEST_CLIENT_NAME)
                .setSubscriptionName(runID);
        final String tableName = createTable(helper);

        Condition condition = Conditions.and(
                Conditions.intrinsic(Intrinsic.TABLE, tableName),
                Conditions.mapBuilder().contains("runID", runID).build(),
                Conditions.mapBuilder().matches("~tags", Conditions.containsAny("re-etl")).build()
        );
        subscribeAndCheckConditions(helper.getSubscriptionName(), condition);
    }

    @Test
    public void testTags() {
        final String expectedTag = "testTags";

        final String allUpdateSubscription = String.format("gk_include_tags_databus_sub_all_updates_%s", runID);

        DataStoreHelper includeTagHelper = new DataStoreHelper(dataStore, placement, "include_tags", "databus", runID)
                .setClientName(DATABUS_TEST_CLIENT_NAME)
                .setSubscriptionName("include_tags_" + runID);

        DataStoreHelper excludeTagHelper = new DataStoreHelper(dataStore, placement, "exclude_tags", "databus", runID)
                .setClientName(DATABUS_TEST_CLIENT_NAME)
                .setSubscriptionName("exclude_tags_" + runID);
        // Create tables
        final String tableName1 = createTable(includeTagHelper);

        final String tableName2 = createTable(excludeTagHelper);

        // Create conditions and subscribe
        Condition includeCondition = Conditions.and(
                Conditions.mapBuilder().matches("~tags", Conditions.containsAny(expectedTag)).build(),
                Conditions.or(
                        Conditions.intrinsic(Intrinsic.TABLE, tableName1),
                        Conditions.intrinsic(Intrinsic.TABLE, tableName2))
        );
        subscribeAndCheckConditions(includeTagHelper.getSubscriptionName(), includeCondition);

        Condition excludeCondition = Conditions.and(
                Conditions.not(Conditions.mapBuilder().matches("~tags", Conditions.containsAny(expectedTag)).build()),
                Conditions.or(
                        Conditions.intrinsic(Intrinsic.TABLE, tableName1),
                        Conditions.intrinsic(Intrinsic.TABLE, tableName2))
        );
        subscribeAndCheckConditions(excludeTagHelper.getSubscriptionName(), excludeCondition);

        Condition allUpdatesConditions = Conditions.or(
                Conditions.intrinsic(Intrinsic.TABLE, tableName1),
                Conditions.intrinsic(Intrinsic.TABLE, tableName2));
        subscribeAndCheckConditions(allUpdateSubscription, allUpdatesConditions);

        // Generate some documents with tags
        final int includeTagDocumentCount = 50;
        DataStoreStreaming.updateAll(dataStore, includeTagHelper.generateUpdatesList(includeTagDocumentCount), new HashSet<String>() {{
            add(expectedTag);
        }});

        // Generate some documents without tags
        final int excludeTagDocumentCount = 40;
        DataStoreStreaming.updateAll(dataStore, excludeTagHelper.generateUpdatesList(excludeTagDocumentCount));

        int totalDocumentCount = includeTagDocumentCount + excludeTagDocumentCount;

        // Check results
        assertEquals(flexibleDatabusPeekCount(databus, includeTagHelper.getSubscriptionName(), includeTagDocumentCount), includeTagDocumentCount,
                "Mismatch of Documents returned by subscription that includes tags");

        assertEquals(flexibleDatabusPeekCount(databus, excludeTagHelper.getSubscriptionName(), excludeTagDocumentCount), excludeTagDocumentCount,
                "Mismatch of Documents returned by subscription that excludes tags");

        assertEquals(flexibleDatabusPeekCount(databus, allUpdateSubscription, totalDocumentCount), totalDocumentCount,
                "Mismatch of Documents returned by subscription that includes all updates");
    }

    @Test
    public void testList() {
        String apiTested = "databus";
        String documentKey = "list_subscription";
        String tableName = uniqueName(documentKey, apiTested, DATABUS_TEST_CLIENT_NAME, runID);
        final List<String> expectedSubscriptionNames = new ArrayList<String>() {{
            add(DATABUS_TEST_CLIENT_NAME + "_list_test_1_sub_" + runID);
            add(DATABUS_TEST_CLIENT_NAME + "_list_test_2_sub_" + runID);
            add(DATABUS_TEST_CLIENT_NAME + "_list_test_3_sub_" + runID);
            add(DATABUS_TEST_CLIENT_NAME + "_list_test_4_sub_" + runID);
            add(DATABUS_TEST_CLIENT_NAME + "_list_test_5_sub_" + runID);
        }};

        createDataTable(dataStore,
                tableName,
                getTemplate("databus", DATABUS_TEST_CLIENT_NAME, runID),
                placement);

        Condition condition = Conditions.intrinsic(Intrinsic.TABLE, tableName);

        expectedSubscriptionNames.forEach(sub -> {
            subscribeAndCheckConditions(sub, condition);
            subscriptionsToCleanupAfterTest.add(sub);
        });

        List<String> returnedSubscriptionNames = new ArrayList<>();
        databus.listSubscriptions(null, 100000).forEachRemaining(subscription -> {
            if (subscription.getName().contains("list_test")) {
                returnedSubscriptionNames.add(subscription.getName());
            }
        });

        LOGGER.debug("returned " + returnedSubscriptionNames);
        LOGGER.debug("expected " + expectedSubscriptionNames);
        assertEquals(returnedSubscriptionNames, expectedSubscriptionNames);
    }

    @Test
    public void testUnsubscribe() {
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "unsubscribe", "databus", runID)
                .setClientName(DATABUS_TEST_CLIENT_NAME)
                .setSubscriptionName(runID);

        final String tableName = createTable(helper);

        Condition condition = Conditions.intrinsic(Intrinsic.TABLE, tableName);
        int documentCount = 10;

        final String subscriptionName = helper.getSubscriptionName();
        subscribeAndCheckConditions(subscriptionName, condition);
        subscriptionsToCleanupAfterTest.add(subscriptionName);

        // Update after subscribe
        helper.setDocumentKey("unsubscribe_subscribed");
        DataStoreStreaming.updateAll(dataStore, helper.generateUpdatesList(documentCount));
        assertEquals(flexibleDatabusPeekCount(databus, subscriptionName, documentCount), documentCount);

        // ack so subscription is back to 0
        List<String> eventKeys = new ArrayList<>();
        databus.peek(subscriptionName, 100).forEachRemaining(event -> eventKeys.add(event.getEventKey()));
        databus.acknowledge(subscriptionName, eventKeys);
        assertEquals(flexibleDatabusPeekCount(databus, subscriptionName, 0), 0);

        // Unsubscribe and check
        databus.unsubscribe(subscriptionName);
        helper.setDocumentKey("unsubscribed_unsubscribed");
        DataStoreStreaming.updateAll(dataStore, helper.generateUpdatesList(documentCount));
        assertEquals(flexibleDatabusPeekCount(databus, subscriptionName, 0), 0);
    }

    @Test
    public void testGetClaimCount() {
        final int documentCount = 500;
        final int batchSize = 100;
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "get_claim_count", "databus", runID)
                .setClientName(DATABUS_TEST_CLIENT_NAME)
                .setSubscriptionName(runID);

        final String tableName = createTable(helper);

        Condition condition = Conditions.intrinsic(Intrinsic.TABLE, tableName);
        subscribeAndCheckConditions(helper.getSubscriptionName(), condition);

        // Generate documents and databus events
        DataStoreStreaming.updateAll(dataStore, helper.generateUpdatesList(documentCount));
        snooze(5); // Give databus to get all events
        assertEquals(flexibleDatabusPeekCount(databus, helper.getSubscriptionName(), documentCount), documentCount);
        List<String> eventKeys = new ArrayList<>();
        // ack events and verify claim count
        for (int i = 1; i <= (documentCount / batchSize); i++) {
            LOGGER.debug("Processing batch #{}", i);
            LOGGER.debug("Pre Poll size: {}", eventKeys.size());
            eventKeys.addAll(pollEventKeys(databus, helper.getSubscriptionName(), Duration.ofMinutes(5), batchSize));
            LOGGER.debug("Post Poll size: {}", eventKeys.size());
            snooze(2);
            long actualClaimed = databus.getClaimCount(helper.getSubscriptionName());
            assertEquals(actualClaimed, i * batchSize);
        }

        databus.acknowledge(helper.getSubscriptionName(), eventKeys);
        snooze(5); // Give databus time to ack all events
        assertEquals(databus.getClaimCount(helper.getSubscriptionName()), 0);
    }

    @Test
    public void testDatabusUnclaimAll() {
        final int documentCount = 100;
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "unclaim_all", "databus", runID)
                .setClientName(DATABUS_TEST_CLIENT_NAME)
                .setSubscriptionName(runID);

        final String tableName = createTable(helper);

        Condition condition = Conditions.intrinsic(Intrinsic.TABLE, tableName);
        subscribeAndCheckConditions(helper.getSubscriptionName(), condition);

        // Generate documents and databus events
        DataStoreStreaming.updateAll(dataStore, helper.generateUpdatesList(documentCount));
        snooze(16); // Give databus to get all events
        assertEquals(flexibleDatabusPeekCount(databus, helper.getSubscriptionName(), documentCount), documentCount);

        // ack events and verify claim count
        databus.poll(helper.getSubscriptionName(), Duration.ofMinutes(5), documentCount);
        assertEquals(databus.getClaimCount(helper.getSubscriptionName()), documentCount);
        databus.unclaimAll(helper.getSubscriptionName());
        snooze(16); // Give databus time to ack all events
        assertEquals(databus.getClaimCount(helper.getSubscriptionName()), 0);

        //Check that you can still claim the events
        databus.poll(helper.getSubscriptionName(), Duration.ofMinutes(5), documentCount);
        assertEquals(databus.getClaimCount(helper.getSubscriptionName()), documentCount);
    }

    @Test
    public void testRenew() {
        final int documentCount = 100;
        final int renewDocCount = 50;
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "renew", "databus", runID)
                .setClientName(DATABUS_TEST_CLIENT_NAME)
                .setSubscriptionName(runID);

        final String tableName = createTable(helper);

        Condition condition = Conditions.intrinsic(Intrinsic.TABLE, tableName);
        subscribeAndCheckConditions(helper.getSubscriptionName(), condition);

        // Generate documents and databus events
        DataStoreStreaming.updateAll(dataStore, helper.generateUpdatesList(documentCount));
        snooze(5); // Give databus to get all events
        assertEquals(flexibleDatabusPeekCount(databus, helper.getSubscriptionName(), documentCount), documentCount);

        // ack events and verify claim count
        List<String> eventKeys = new ArrayList<>();
        databus.poll(helper.getSubscriptionName(), Duration.ofSeconds(15), documentCount).getEventIterator().forEachRemaining(
                event -> eventKeys.add(event.getEventKey())
        );
        assertEquals(databus.getClaimCount(helper.getSubscriptionName()), documentCount);
        snooze(5);

        List<String> renewKeyList = eventKeys.subList(0, renewDocCount);
        databus.renew(helper.getSubscriptionName(), renewKeyList, Duration.ofSeconds(15));
        snooze(11); // Wait out original claim
        assertEquals(databus.getClaimCount(helper.getSubscriptionName()), renewDocCount);

        databus.poll(helper.getSubscriptionName(), Duration.ofSeconds(2), documentCount).getEventIterator().forEachRemaining(
                event -> assertFalse(renewKeyList.contains(event.getEventKey()), "Renew event key found in list of unclaimed.")
        );

        // Check events do expire
        snooze(5); // Wait out second claim
        assertEquals(databus.getClaimCount(helper.getSubscriptionName()), 0);
    }

    @Test
    public void testPurge() {
        final int documentCount = 100;
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "purge", "databus", runID)
                .setClientName(DATABUS_TEST_CLIENT_NAME)
                .setSubscriptionName(runID);

        final String tableName = createTable(helper);

        Condition condition = Conditions.intrinsic(Intrinsic.TABLE, tableName);
        subscribeAndCheckConditions(helper.getSubscriptionName(), condition);

        // Generate documents and databus events
        DataStoreStreaming.updateAll(dataStore, helper.generateUpdatesList(documentCount));
        snooze(5); // Give databus to get all events
        assertEquals(flexibleDatabusPeekCount(databus, helper.getSubscriptionName(), documentCount), documentCount);

        // ack events and verify claim count
        databus.poll(helper.getSubscriptionName(), Duration.ofMinutes(5), documentCount);
        assertEquals(databus.getClaimCount(helper.getSubscriptionName()), documentCount);
        databus.purge(helper.getSubscriptionName());
        snooze(5); // Give databus time to ack all events
        assertEquals(databus.getClaimCount(helper.getSubscriptionName()), 0);

        //Check that you can't still claim the events
        assertEquals(Lists.newArrayList(databus.poll(helper.getSubscriptionName(), Duration.ofMinutes(5), documentCount).getEventIterator()).size(), 0);
        assertEquals(databus.getClaimCount(helper.getSubscriptionName()), 0);
    }

    @Test
    public void testPeek() {
        final int documentCount = 100;
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "peek", "databus", runID)
                .setClientName(DATABUS_TEST_CLIENT_NAME)
                .setSubscriptionName(runID);

        final String tableName = createTable(helper);

        Condition condition = Conditions.intrinsic(Intrinsic.TABLE, tableName);
        subscribeAndCheckConditions(helper.getSubscriptionName(), condition);

        // Generate documents and databus events
        DataStoreStreaming.updateAll(dataStore, helper.generateUpdatesList(documentCount));
        snooze(10); // Give databus to get all events
        assertEquals(flexibleDatabusPeekCount(databus, helper.getSubscriptionName(), documentCount), documentCount);

        // ack events and verify claim count
        List<String> eventKeys = pollEventKeys(databus, helper.getSubscriptionName(), Duration.ofMinutes(5), documentCount);
        assertEquals(databus.getClaimCount(helper.getSubscriptionName()), documentCount);
        assertEquals(flexibleDatabusPeekCount(databus, helper.getSubscriptionName(), documentCount), documentCount); // Return all un ack'ed events
        databus.acknowledge(helper.getSubscriptionName(), eventKeys);
        snooze(5);
        assertEquals(flexibleDatabusPeekCount(databus, helper.getSubscriptionName(), 0), 0);
    }

    @Test(enabled = false)
    public void testReplay() {
        final int documentCount = 100;
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "replay", "databus", runID)
                .setClientName(DATABUS_TEST_CLIENT_NAME)
                .setSubscriptionName(runID);
        final String tableName = createTable(helper);

        Condition condition = Conditions.intrinsic(Intrinsic.TABLE, tableName);
        subscribeAndCheckConditions(helper.getSubscriptionName(), condition);

        DataStoreStreaming.updateAll(dataStore, helper.generateUpdatesList(documentCount));
        snooze(5);
        assertEquals(flexibleDatabusPeekCount(databus, helper.getSubscriptionName(), documentCount), documentCount);

        List<Event> pollEvents = Lists.newArrayList(flexiblePoll(databus, helper.getSubscriptionName(), Duration.ofMinutes(5), documentCount).getEventIterator());
        List<Map<String, Object>> preReplayEventContents = pollEvents.stream().map(Event::getContent).collect(Collectors.toList());
        databus.acknowledge(helper.getSubscriptionName(), pollEvents.stream().map(Event::getEventKey).collect(Collectors.toList()));
        assertEquals(preReplayEventContents.size(), documentCount);

        String replaySubscriptionName = String.format("gk_%s_%s_sub_%s", "databus", "replay_2", runID);
        subscribeAndCheckConditions(replaySubscriptionName, condition);
        assertEquals(flexibleDatabusPeekCount(databus, replaySubscriptionName, 0), 0);

        String replayReference = databus.replayAsync(replaySubscriptionName);
        do {
            LOGGER.info("Waiting for replay to finish!");
            snooze(1);
        } while (databus.getReplayStatus(replayReference).getStatus() == ReplaySubscriptionStatus.Status.IN_PROGRESS);
        snooze(2);
        assertEquals(databus.getReplayStatus(replayReference).getStatus(), ReplaySubscriptionStatus.Status.COMPLETE);

        assertEquals(flexibleDatabusPeekCount(databus, helper.getSubscriptionName(), 0), 0);
        assertEquals(flexibleDatabusPeekCount(databus, replaySubscriptionName, documentCount), documentCount);
        pollEvents = Lists.newArrayList(flexiblePoll(databus, replaySubscriptionName, Duration.ofMinutes(5), documentCount).getEventIterator());
        List<Map<String, Object>> postReplayEventContents = pollEvents.stream().map(Event::getContent).collect(Collectors.toList());
        assertEquals(postReplayEventContents, preReplayEventContents);
    }

    /*
        Because of the way replay works, there might be upto 1000 events from first batch
     */
    @Test
    public void testReplaySince() {
        final int firstBatch = 2000;
        final int documentCount = 550;
        final int totalDocCount = firstBatch + documentCount;
        final int limit = 500;
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "replay_since", "databus", runID)
                .setClientName(DATABUS_TEST_CLIENT_NAME)
                .setSubscriptionName(runID);
        final String tableName = createTable(helper);

        Condition condition = Conditions.intrinsic(Intrinsic.TABLE, tableName);
        subscribeAndCheckConditions(helper.getSubscriptionName(), condition);

        DataStoreStreaming.updateAll(dataStore, helper.generateUpdatesList(firstBatch));
        Date midwayMark = new Date();
        snooze(10);

        final String newDocumentKey = "replay_since_post_ts";
        helper.setDocumentKey(newDocumentKey);
        List<Update> postTSUpdate = helper.generateUpdatesList(documentCount);
        List<String> expectedKeys = postTSUpdate.stream().map(Update::getKey).collect(Collectors.toList());
        DataStoreStreaming.updateAll(dataStore, postTSUpdate);
        snooze(10);
        int totalEvents = 0;
        int pollAttempt = 1;
        PollResult result = flexiblePoll(databus, helper.getSubscriptionName(), Duration.ofMinutes(5), limit);
        do {
            int previousEventSize = totalEvents;
            List<Event> pollEvents = Lists.newArrayList(result.getEventIterator());
            databus.acknowledge(helper.getSubscriptionName(), pollEvents.stream().map(Event::getEventKey).collect(Collectors.toList()));
            totalEvents += pollEvents.size();
            LOGGER.info("Iteration #{} - totalEvents: {}", pollAttempt, totalEvents);
            if (previousEventSize == totalEvents) {
                pollAttempt++;
            }
            result = flexiblePoll(databus, helper.getSubscriptionName(), Duration.ofMinutes(5), limit);
        } while (totalEvents < totalDocCount && pollAttempt <= 10);
//        assertTrue(totalEvents >= totalDocCount, String.format("totalEvents: %s - totalDocCount %s", totalEvents, totalDocCount));
        String replaySubscriptionName = String.format("gk_%s_%s_sub_%s", "databus", "replay_post_ts", runID);
        subscribeAndCheckConditions(replaySubscriptionName, condition);
        assertEquals(flexibleDatabusPeekCount(databus, replaySubscriptionName, 0), 0);

        String replayReference = databus.replayAsyncSince(replaySubscriptionName, midwayMark);
        do {
            LOGGER.info("Waiting for replay to finish!");
            snooze(1);
        } while (databus.getReplayStatus(replayReference).getStatus() == ReplaySubscriptionStatus.Status.IN_PROGRESS);
        snooze(2);
        assertEquals(databus.getReplayStatus(replayReference).getStatus(), ReplaySubscriptionStatus.Status.COMPLETE);

        pollAttempt = 1;
        List<Event> replayEvents = new ArrayList<>();
        result = flexiblePoll(databus, replaySubscriptionName, Duration.ofMinutes(5), limit);
        do {
            int previousEventSize = replayEvents.size();
            replayEvents.addAll(Lists.newArrayList(result.getEventIterator()));
            LOGGER.info("Iteration #{} - totalEvents: {}", pollAttempt, replayEvents.size());
            if (previousEventSize == replayEvents.size()) {
                pollAttempt++;
            }
            result = flexiblePoll(databus, replaySubscriptionName, Duration.ofMinutes(5), limit);
        } while (replayEvents.size() < documentCount + 1000 && pollAttempt <= 10);

        LOGGER.info("replayEvents.size: " + replayEvents.size());
        List<String> actualKeys = replayEvents.stream().map(Event::getContent).map(Intrinsic::getId).collect(Collectors.toList());
        List<String> nonFoundKeys = new ArrayList<>();
        expectedKeys.forEach(key -> {
            if (!actualKeys.contains(key)) {
                nonFoundKeys.add(key);
            }
        });
        assertEquals(nonFoundKeys.size(), 0, "Following keys not found: " + nonFoundKeys);
    }

    @Test
    public void testMove() {
        final int documentCount = 2674;
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "move", "databus", runID)
                .setClientName(DATABUS_TEST_CLIENT_NAME)
                .setSubscriptionName(runID);
        final String tableName = createTable(helper);

        String moveToSubscription = String.format("gk_%s_%s_sub_%s", "databus", "move_to", runID);
        Condition condition = Conditions.intrinsic(Intrinsic.TABLE, tableName);

        subscribeAndCheckConditions(helper.getSubscriptionName(), condition);
        subscribeAndCheckConditions(moveToSubscription, Conditions.alwaysFalse());

        List<Update> updateList = helper.generateUpdatesList(documentCount);
        DataStoreStreaming.updateAll(dataStore, updateList);

        Stopwatch pollTimer = Stopwatch.createStarted();
        List<Event> pollResults = new ArrayList<>();
        do {
            LOGGER.debug("[1] pollResults.size: " + pollResults.size());
            LOGGER.debug("time spent: " + pollTimer.elapsed(TimeUnit.SECONDS));
            pollResults.addAll(Lists.newArrayList(databus.poll(helper.getSubscriptionName(), Duration.ofMinutes(5), 500).getEventIterator()));
            snooze(1);
        } while (pollTimer.elapsed(TimeUnit.SECONDS) < 30 && pollResults.size() < documentCount);
        assertEquals(pollResults.size(), documentCount);
        databus.unclaimAll(helper.getSubscriptionName());

        subscribeAndCheckConditions(helper.getSubscriptionName(), Conditions.alwaysFalse());
        subscribeAndCheckConditions(moveToSubscription, condition);

        String moveReference = databus.moveAsync(helper.getSubscriptionName(), moveToSubscription);

        do {
            LOGGER.info("Waiting for move to finish!");
            snooze(1);
        } while (databus.getMoveStatus(moveReference).getStatus() == MoveSubscriptionStatus.Status.IN_PROGRESS);
        snooze(2);
        assertEquals(databus.getMoveStatus(moveReference).getStatus(), MoveSubscriptionStatus.Status.COMPLETE);

        pollTimer.reset().start();
        List<Event> movePollResults = new ArrayList<>();
        LOGGER.info("Polling for events...");
        do {
            LOGGER.debug("[2] pollResults.size: " + movePollResults.size());
            LOGGER.debug("time spent: " + pollTimer.elapsed(TimeUnit.SECONDS));
            databus.poll(moveToSubscription, Duration.ofMinutes(5), 500).getEventIterator().forEachRemaining(event -> {
                movePollResults.add(event);
                databus.acknowledge(moveToSubscription, Lists.newArrayList(event.getEventKey()));
            });
            snooze(1);
        } while (pollTimer.elapsed(TimeUnit.SECONDS) < 30 && movePollResults.size() < documentCount);
        assertEquals(movePollResults.size(), documentCount);
        final Set<String> expected = updateList.stream().map(Update::getKey).collect(Collectors.toSet());
        final Set<String> actual = movePollResults.stream().map(Event::getContent).map(Intrinsic::getId).collect(Collectors.toSet());
        assertEquals(actual, expected);
    }

    @Test
    public void testDedup() {
        final int documentCount = 100;
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "dedup", "databus", runID)
                .setSubscriptionName(runID)
                .setClientName(DATABUS_TEST_CLIENT_NAME);
        final String tableName = createTable(helper);

        Condition condition = Conditions.intrinsic(Intrinsic.TABLE, tableName);
        subscribeAndCheckConditions(helper.getSubscriptionName(), condition);

        List<Update> updates = helper.generateUpdatesList(documentCount);

        IntStream.rangeClosed(1, 8)
                .forEach(i -> DataStoreStreaming.updateAll(dataStore, updates));
        assertEquals(flexibleDatabusPeekCount(databus, helper.getSubscriptionName(), documentCount * 2), documentCount);
    }

    private static long protectedDatabusGetCount(Databus databus, String subscription) {
        int exceptionCount = 0;
        while (true) {
            try {
                return databus.getEventCount(subscription);
            } catch (Exception ex) {
                assertTrue(++exceptionCount < 10, "too many Databus.getCount exceptions ... giving up");
                LOGGER.warn("caught {} ({}) when getting count ... retry {} ...", ex.getClass().getName(), ex.getMessage(), exceptionCount);
            }
        }
    }

    @Test(groups = "databus.api", timeOut = 3600000)
    public void testSubscriptionUsingDocumentPartitioning() {
        final String baseName = "databus_partitioning";
        final String dataType = "partition";
        final List<String> subscriptions = ImmutableList.of(
                baseName + "_sub_1__" + runID,
                baseName + "_sub_2__" + runID);

        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "partitioned", "databus", runID)
                .setSubscriptionName(runID)
                .setClientName(DATABUS_TEST_CLIENT_NAME);

        final String tableName = createTable(helper);

        // create source queue and destination queues
        for (int i = 0; i < 2; i++) {
            Condition condition = Conditions.and(
                    Conditions.intrinsic(Intrinsic.TABLE, tableName),
                    Conditions.partition(2, Conditions.equal(i + 1)));

            String subscription = subscriptions.get(i);
            subscribeAndCheckConditions(subscription, condition);
            assertEquals(protectedDatabusGetCount(databus, subscription), 0, "Just-created subscription should be empty");
        }

        // add 50 documents
        final Audit audit = new AuditBuilder().setComment("testDocIdHash").build();
        List<Update> updates = IntStream.range(0, 50).mapToObj(i ->
                new Update(tableName, String.format("document-%02d", i), null,
                        Deltas.mapBuilder()
                                .put("index", i)
                                .put("type", dataType)
                                .build(), audit))
                .collect(Collectors.toList());
        DataStoreStreaming.updateAll(dataStore, updates);

        // Brief snooze as fanout starts
        snooze(1);

        // Poll both subscriptions until all 50 documents come back.
        Multimap<String, String> documents = TreeMultimap.create();
        Stopwatch stopwatch = Stopwatch.createStarted();
        while (documents.size() < 50 && stopwatch.elapsed(TimeUnit.SECONDS) < 60) {
            snooze(1);
            subscriptions.forEach(subscription -> {
                PollResult pollResult = databus.poll(subscription, Duration.ofHours(1), 50);
                final List<String> eventKeys = new ArrayList<>();
                pollResult.getEventIterator().forEachRemaining(event -> {
                    documents.put(subscription, Intrinsic.getId(event.getContent()));
                    eventKeys.add(event.getEventKey());
                });
                if (!eventKeys.isEmpty()) {
                    databus.acknowledge(subscription, eventKeys);
                }
            });
        }

        // We don't know what the exact breakdown by subscription will be, but the documents should be split between
        // the two subscriptions fairly evenly.  Check that each subscription has at least 10 documents of the 50.
        subscriptions.forEach(subscription -> assertTrue(documents.get(subscription).size() >= 10));
        // Verify each document appears exactly once in either of the subscriptions
        IntStream.range(0, 50).forEach(i -> {
            String documentId = String.format("document-%02d", i);
            assertEquals(
                    subscriptions.stream()
                            .filter(subscription -> documents.get(subscription).contains(documentId))
                            .count(),
                    1);
        });
    }

    @Test
    public void testPermissions_list_subs() {
        String key0 = createNewDatabusApiKey(uac);
        final String sub0 = uniqueName("permission_list_owned_subs", "databus", null, runID) + "0";
        final Databus bus0 = createNewDatabusClient(key0, clusterName, emodbHost);
        subscribeAndCheckConditions(bus0, sub0, Conditions.alwaysTrue());

        String key1 = createNewDatabusApiKey(uac);
        final String sub1 = uniqueName("permission_list_owned_subs", "databus", null, runID) + "1";
        final Databus bus1 = createNewDatabusClient(key1, clusterName, emodbHost);
        subscribeAndCheckConditions(bus1, sub1, Conditions.alwaysTrue());

        // Verify each API key can only see the subscription it created
        List<Subscription> subscriptions = ImmutableList.copyOf(bus0.listSubscriptions(null, 10));
        assertEquals(subscriptions.size(), 1);
        assertEquals(subscriptions.get(0).getName(), sub0);

        subscriptions = ImmutableList.copyOf(bus1.listSubscriptions(null, 10));
        assertEquals(subscriptions.size(), 1);
        assertEquals(subscriptions.get(0).getName(), sub1);
    }

    @Test
    public void testPermissions_claim_sub() {
        String sub = uniqueName("permission_claim_sub", "databus", null, runID);
        String key0 = createNewDatabusApiKey(uac);
        final Databus bus0 = createNewDatabusClient(key0, clusterName, emodbHost);

        String key1 = createNewDatabusApiKey(uac);
        final Databus bus1 = createNewDatabusClient(key1, clusterName, emodbHost);

        subscribeAndCheckConditions(bus0, sub, Conditions.alwaysTrue());

        try {
            bus1.subscribe(sub, Conditions.alwaysTrue(), Duration.ofMinutes(60), Duration.ofMinutes(10));
            fail("key1 should not have permission to renew to key0's subscription");
        } catch (UnauthorizedSubscriptionException e) {
            assertEquals(e.getSubscription(), sub);
        }

        bus0.subscribe(sub, Conditions.alwaysTrue(), Duration.ofMinutes(60), Duration.ofMinutes(10));
    }

    @Test
    public void testPermissions_delete_sub() {
        String sub = uniqueName("permission_delete_sub", "databus", null, runID);
        String key0 = createNewDatabusApiKey(uac);
        final Databus bus0 = createNewDatabusClient(key0, clusterName, emodbHost);

        String key1 = createNewDatabusApiKey(uac);
        final Databus bus1 = createNewDatabusClient(key1, clusterName, emodbHost);

        subscribeAndCheckConditions(bus0, sub, Conditions.alwaysTrue());

        try {
            bus1.unsubscribe(sub);
            fail("key1 should not have permission to delete key0's subscription");
        } catch (UnauthorizedSubscriptionException e) {
            assertEquals(e.getSubscription(), sub);
        }

        bus0.unsubscribe(sub);
        try {
            databus.getSubscription(sub);
            fail("Subscription should no longer exist after unsubscribe by key0");
        } catch (UnknownSubscriptionException e) {
            assertEquals(e.getSubscription(), sub);
        }
    }

    @Test
    public void testPermissions_move_sub() {
        String key0 = createNewDatabusApiKey(uac);
        final String sub0 = uniqueName("permission_move_subs", "databus", null, runID) + "0";
        final Databus bus0 = createNewDatabusClient(key0, clusterName, emodbHost);
        subscribeAndCheckConditions(bus0, sub0, Conditions.alwaysTrue());

        String key1 = createNewDatabusApiKey(uac);
        final String sub1 = uniqueName("permission_move_subs", "databus", null, runID) + "1";
        final Databus bus1 = createNewDatabusClient(key1, clusterName, emodbHost);
        subscribeAndCheckConditions(bus1, sub1, Conditions.alwaysTrue());

        // key0 should not be able to move from or to sub1
        for (boolean fromSub0 : ImmutableList.of(true, false)) {
            String from = fromSub0 ? sub0 : sub1;
            String to = fromSub0 ? sub1 : sub0;
            try {
                bus0.moveAsync(from, to);
                fail("key0 should not have permission to move subscription " + from + " to " + to);
            } catch (UnauthorizedSubscriptionException e) {
                assertEquals(e.getSubscription(), sub1);
            }
        }
    }

    @Test
    public void testPermissions_poll_sub() {
        String sub = uniqueName("permission_poll_sub", "databus", null, runID);
        String key0 = createNewDatabusApiKey(uac);
        final Databus bus0 = createNewDatabusClient(key0, clusterName, emodbHost);

        String key1 = createNewDatabusApiKey(uac);
        final Databus bus1 = createNewDatabusClient(key1, clusterName, emodbHost);

        subscribeAndCheckConditions(bus0, sub, Conditions.alwaysTrue());

        // Verify only key0 can peek or poll the subscription
        for (String op : ImmutableList.of("peek", "poll")) {
            try {
                if (op.equals("peek")) {
                    bus1.peek(sub, 10);
                } else {
                    bus1.poll(sub, Duration.ofMinutes(1), 10);
                }
                fail("key1 should not have permission to " + op + " key0's subscription");
            } catch (UnauthorizedSubscriptionException e) {
                assertEquals(e.getSubscription(), sub);
            }
        }

        // The subscription doesn't have any read permissions so no results are excepted.
        assertFalse(bus0.peek(sub, 10).hasNext());
        assertEquals(bus0.poll(sub, Duration.ofMinutes(1), 10), 0);
    }

    @Test(enabled = false)
    public void testPermissions_replay_sub() {
        final Date dateTimeNow = new Date();

        // Create two tables, only one of which matches the role's read permission
        DataStoreHelper readableHelper = new DataStoreHelper(dataStore, placement, "permission_replay_sub_readable", "databus", runID)
                .setClientName(DATABUS_TEST_CLIENT_NAME).setSubscriptionName(runID);
        final String tableName1 = createTable(readableHelper);

        DataStoreHelper unreadableHelper = new DataStoreHelper(dataStore, placement, "permission_replay_sub_unreadable", "databus", runID)
                .setClientName(DATABUS_TEST_CLIENT_NAME).setSubscriptionName(runID);
        final String tableName2 = createTable(unreadableHelper);

        // Create a role that only has read permission on a single table
        String role = "databus_role_replay_owned_subscription_" + runID;
        createNewDatabusRole(uac, role, Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal(tableName1)));

        // Create an API key with the previous role
        String key = createNewDatabusApiKey(uac, role);
        Databus bus = createNewDatabusClient(key, clusterName, emodbHost);
        // Write multiple updates to both tables
        DataStoreStreaming.updateAll(dataStore, readableHelper.generateUpdatesList(10));
        DataStoreStreaming.updateAll(dataStore, unreadableHelper.generateUpdatesList(10));
        Set<String> expectedKeys = Sets.newHashSet();
        for (int i = 1; i <= 10; i++) {
            expectedKeys.add(String.format("%s-%s", readableHelper.getDocumentKey(), i));
        }
        // Short wait to let the updates settle
        snooze(5);

        String sub = "databus_test_replay_owned_" + runID;
        subscribeAndCheckConditions(sub, Conditions.alwaysTrue());
        assertEquals(bus.getEventCountUpTo(sub, 100), 0, "subscription should not contain events");

        String replayReference = databus.replayAsyncSince(sub, dateTimeNow);
        do {
            LOGGER.info("Waiting for replay to finish!");
            snooze(5);
        } while (databus.getReplayStatus(replayReference).getStatus() == ReplaySubscriptionStatus.Status.IN_PROGRESS);
        assertEquals(databus.getReplayStatus(replayReference).getStatus(), ReplaySubscriptionStatus.Status.COMPLETE);
        snooze(10);

        PollResult pollResult = flexiblePoll(bus, sub, Duration.ofMinutes(5), 100, 10);
        LOGGER.info("hasMoreEvents: " + pollResult.hasMoreEvents());
        List<Event> replayEvents = Lists.newArrayList(pollResult.getEventIterator());
        LOGGER.info("replayEvents.size: " + replayEvents.size());
        List<String> actualKeys = replayEvents.stream().map(Event::getContent).map(Intrinsic::getId).collect(Collectors.toList());
        assertEquals(actualKeys, expectedKeys, "Not all the keys found in actualKeys");
    }

    @Test
    public void testPermissions_poll_limited_read() {
        DataStoreHelper readableHelper = new DataStoreHelper(dataStore, placement, "permission_limited_read_readable", "databus", runID)
                .setClientName(DATABUS_TEST_CLIENT_NAME).setSubscriptionName(runID);
        final String readableHelperTable = createTable(readableHelper);

        DataStoreHelper unreadableHelper = new DataStoreHelper(dataStore, placement, "permission_limited_read_unreadable", "databus", runID)
                .setClientName(DATABUS_TEST_CLIENT_NAME).setSubscriptionName(runID);
        final String unreadableHelperTable = createTable(unreadableHelper);

        String role = "databus_role_poll_limieted_read";
        createNewDatabusRole(uac, role, Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal(readableHelperTable)));
        String key = createNewDatabusApiKey(uac, role);
        Databus bus = createNewDatabusClient(key, clusterName, emodbHost);

        // Subscribe to all updates
        String sub = "databus_test_poll_owned_" + runID;
        subscribeAndCheckConditions(bus, sub, Conditions.alwaysTrue());

        List<Update> expectedUpdate = readableHelper.generateUpdatesList(10);
        DataStoreStreaming.updateAll(dataStore, expectedUpdate);
        DataStoreStreaming.updateAll(dataStore, unreadableHelper.generateUpdatesList(10));

        //Save keys to check for later
        Set<String> expectedKeys = expectedUpdate
                .stream()
                .map(Update::getKey)
                .collect(Collectors.toSet());

        // Wait a reasonable amount of time for the updates to appear in the subscription
        assertEquals(flexibleDatabusPeekCount(bus, sub, 10), 10, "limited bus should return 10 events exactly");

        List<Event> polledEvents = Lists.newArrayList(flexiblePoll(bus, sub, Duration.ofMinutes(5), 100).getEventIterator());
        Set<String> actualKeys = polledEvents
                .stream()
                .map(Event::getContent)
                .map(Intrinsic::getId)
                .collect(Collectors.toSet());

        assertEquals(actualKeys, expectedKeys, "Not all the keys found in actualKeys");
    }

    private static void createNewDatabusRole(UserAccessControl uac, String role, Condition readCondition) {
        createRole(uac, role, ImmutableSet.of(
                "databus|*",    // Full databus permissions
                "sor|read|if(" + readCondition + ")"    // Permission to read only with the given condition
        ));
    }

    private static String createNewDatabusApiKey(UserAccessControl uac) {
        return createNewDatabusApiKey(uac, "databus_standard");
    }

    private static String createNewDatabusApiKey(UserAccessControl uac, String... roles) {
        String key = padKey(String.format("DatabusKey%04d", NEXT_API_KEY.getAndIncrement()));
        createApiKey(uac, key, "data-team-qa_DatabusTests", "data-team-qa_DatabusTests", ImmutableSet.copyOf(roles));
        return key;
    }

    private void subscribeAndCheckConditions(String subscription, Condition condition) {
        subscribeAndCheckConditions(databus, subscription, condition);
    }

    private void subscribeAndCheckConditions(Databus databus, String subscription, Condition condition) {
        subscribeAndCheckConditions(databus, subscription, condition, 60, 10);
    }

    private void subscribeAndCheckConditions(Databus databus, String subscription, Condition condition, int subTtl, int eventTtl) {
        databus.subscribe(subscription, condition, Duration.ofMinutes(subTtl), Duration.ofMinutes(eventTtl), false);
        subscriptionsToCleanupAfterTest.add(subscription);
        snooze(1);
        assertEquals(databus.getSubscription(subscription).getName(), subscription);
        assertEquals(databus.getSubscription(subscription).getTableFilter(), condition);
    }

    private static int flexibleDatabusPeekCount(Databus databus, String subscription, int minPeekCount) {
        return flexibleDatabusPeekCount(databus, subscription, 100, minPeekCount);
    }

    private static int flexibleDatabusPeekCount(Databus databus, String subscription, int limit, int minPeekCount) {
        // Be flexible with the amount of asynchronous time it takes for EmoDB to add the update events to the databus
        snooze(3);
        limit = Math.max(limit, minPeekCount);
        Stopwatch peekTimer = Stopwatch.createStarted();
        int peekCount;
        while ((peekCount = Iterators.size(databus.peek(subscription, limit))) < minPeekCount && peekTimer.elapsed(TimeUnit.SECONDS) < 45) {
            LOGGER.info("Got size of " + peekCount + " for " + subscription);
            snooze(2);
        }
        return peekCount;
    }

    private static PollResult flexiblePoll(Databus databus, String subscription, Duration duration, int limit) {
        return flexiblePoll(databus, subscription, duration, limit, limit);
    }

    private static PollResult flexiblePoll(Databus databus, String subscription, Duration duration, int limit, int minPollCount) {
        ArrayList<Event> eventList = new ArrayList<>();
        Boolean hasMoreEvents = null;

        assertTrue(limit >= minPollCount,
                String.format("Can't have limit parameter lower than minPollCount. limit: %s, minPollCount: %s", limit, minPollCount));

        Stopwatch pollTimer = Stopwatch.createStarted();
        PollResult result;

        while ((eventList.size()) < minPollCount && pollTimer.elapsed(TimeUnit.SECONDS) < 30) {
            LOGGER.debug("Polling for {} events with minimum count of {}", limit, minPollCount);
            result = databus.poll(subscription, duration, limit - eventList.size());
            Iterators.addAll(eventList, result.getEventIterator());
            hasMoreEvents = result.hasMoreEvents();
            LOGGER.debug("hasMoreEvents: {}", hasMoreEvents);
            snooze(1);
        }

        return new PollResult(eventList.iterator(), eventList.size(), hasMoreEvents);
    }

    private static List<String> pollEventKeys(Databus databus, String subscription, Duration duration, int batchSize) {
        List<String> eventKeys = new ArrayList<>();
        flexiblePoll(databus, subscription, duration, batchSize).getEventIterator().forEachRemaining(
                event -> eventKeys.add(event.getEventKey())
        );
        return eventKeys;
    }

    private static Databus createNewDatabusClient(String apiKey, String clusterName, String emodbHost) {
        MetricRegistry metricRegistry = new MetricRegistry(); // This is usually a singleton passed

        return ServicePoolBuilder.create(Databus.class)
                .withHostDiscoverySource(new DatabusFixedHostDiscoverySource(emodbHost))
                .withServiceFactory(DatabusClientFactory.forCluster(clusterName, new MetricRegistry()).usingCredentials(apiKey))
                .withMetricRegistry(metricRegistry)
                .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));
    }
}
