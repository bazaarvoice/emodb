package com.bazaarvoice.gatekeeper.emodb.tests.core;

import com.bazaarvoice.emodb.queue.api.DedupQueueService;
import com.bazaarvoice.emodb.queue.api.Message;
import com.bazaarvoice.gatekeeper.emodb.commons.TestModuleFactory;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.bazaarvoice.emodb.queue.api.MoveQueueStatus.Status.COMPLETE;
import static com.bazaarvoice.gatekeeper.emodb.commons.utils.Names.uniqueName;
import static com.bazaarvoice.gatekeeper.emodb.commons.utils.QueueServiceUtils.getMessageList;
import static com.bazaarvoice.gatekeeper.emodb.commons.utils.RetryUtils.snooze;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = {"emodb.core.all", "emodb.core.dedupqueueservice", "dedupqueueservice"}, timeOut = 360000)
@Guice(moduleFactory = TestModuleFactory.class)
public class DedupQueueTests {
    private static final Logger LOGGER = LoggerFactory.getLogger(DedupQueueTests.class);
    private static final String DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME = "gatekeeper_dedup_queue_client";

    @Inject
    private DedupQueueService dedupQueueService;

    @Inject
    @Named("runID")
    private String runID;
    private Set<String> queuesToCleanup;

    @BeforeTest(alwaysRun = true)
    public void beforeTest() {
        queuesToCleanup = new HashSet<>();
    }

    @AfterTest(alwaysRun = true)
    public void afterTest() {
        queuesToCleanup.forEach(this::purgeQueue);
    }

    @Test
    public void testDedupSend() {
        final String queueName = getQueueName("dedup_queue", "send", DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME);

        // Send message
        final List<Object> messages = getMessageList(DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME, runID, "send", null, 1);
        Map<Object, Object> message = (Map<Object, Object>) messages.get(0);
        LOGGER.info("message: {}", message);
        dedupQueueService.send(queueName, message);
        snooze(1);
        dedupQueueService.send(queueName, message);

        // Verify message
        List<Message> peekList = flexibleDedupQueuePeek(dedupQueueService, queueName, 10);
        assertEquals(dedupQueueService.getMessageCount(queueName), 1, "Expected 1 message in queue");
        assertEquals(peekList.size(), 1, "Expected 1 message returned from peek");
        assertEquals(peekList.get(0).getPayload(), message, "Found mismatch on sent message.");
    }

    @Test
    public void testDedupSendAll_multipleMessages() {
        final String queueName = getQueueName("multiple_messages", "send_all", DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME);
        List<Object> messages = getMessageList(DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME, runID, "send_all", Arrays.asList("condition", "multiple_msgs"), 10);
        dedupQueueService.sendAll(queueName, messages);
        snooze(1);
        dedupQueueService.sendAll(queueName, messages);

        // Verify message
        List<Object> actualMessageList = flexibleDedupQueuePeek(dedupQueueService, queueName, 10)
                .stream()
                .map(Message::getPayload)
                .collect(Collectors.toList());
        assertEquals(dedupQueueService.getMessageCount(queueName), 10, "Expected 1 message in queue");
        assertEquals(actualMessageList, messages);
    }

    @Test
    public void testDedupSendAll_multipleQueues() {
        final String queueOneName = getQueueName("multiple_queues_1", "send_all", DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME);
        final String queueTwoName = getQueueName("multiple_queues_2", "send_all", DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME);
        final String queueThreeName = getQueueName("multiple_queues_3", "send_all", DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME);

        List<Object> queueOneMessageList = getMessageList(DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME, runID, "send_all",
                Arrays.asList("condition", "multiple_queues", "dedup_queue", "1"), 1);
        List<Object> queueTwoMessageList = getMessageList(DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME, runID, "send_all",
                Arrays.asList("condition", "multiple_queues", "dedup_queue", "2"), 2);
        List<Object> queueThreeMessageList = getMessageList(DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME, runID, "send_all",
                Arrays.asList("condition", "multiple_queues", "dedup_queue", "3"), 3);

        Map<String, List<Object>> combinedJson = ImmutableMap.<String, List<Object>>builder()
                .put(queueOneName, queueOneMessageList)
                .put(queueTwoName, queueTwoMessageList)
                .put(queueThreeName, queueThreeMessageList)
                .build();

        dedupQueueService.sendAll(combinedJson);
        snooze(1);
        dedupQueueService.sendAll(combinedJson);

        List<Object> actualMessageListOne = flexibleDedupQueuePeek(dedupQueueService, queueOneName, 10)
                .stream()
                .map(Message::getPayload)
                .collect(Collectors.toList());
        assertEquals(dedupQueueService.getMessageCount(queueOneName), 1);
        assertEquals(actualMessageListOne, queueOneMessageList);

        List<Object> actualMessageListTwo = flexibleDedupQueuePeek(dedupQueueService, queueTwoName, 10)
                .stream()
                .map(Message::getPayload)
                .collect(Collectors.toList());
        assertEquals(dedupQueueService.getMessageCount(queueTwoName), 2);
        assertEquals(actualMessageListTwo, queueTwoMessageList);

        List<Object> actualMessageListThree = flexibleDedupQueuePeek(dedupQueueService, queueThreeName, 10)
                .stream()
                .map(Message::getPayload)
                .collect(Collectors.toList());
        assertEquals(dedupQueueService.getMessageCount(queueThreeName), 3);
        assertEquals(actualMessageListThree, queueThreeMessageList);
    }

    @Test
    public void testDedupGetMessageCount() {
        final String queueName = getQueueName("dedup_queue", "get_message_count", DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME);
        final int totalMessages = 101;

        List<Object> messages = getMessageList(DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME, runID, "getMessageCountUpTo", null, totalMessages);
        dedupQueueService.sendAll(queueName, messages);
        snooze(1);
        dedupQueueService.sendAll(queueName, messages);
        assertEquals(flexibleDedupQueuePeek(dedupQueueService, queueName, totalMessages).size(), totalMessages);
        assertTrue(dedupQueueService.getMessageCount(queueName) >= 1, "getMessageCount should return >= " + totalMessages + " docs");
        assertEquals(dedupQueueService.getMessageCount(queueName), totalMessages, "Higher limit should find " + totalMessages + " messages");
    }

    @Test
    public void testDedupGetMessageCountUpTo() {
        final String queueName = getQueueName("dedup_queue", "get_message_count_up_to", DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME);
        final int totalMessages = 101;

        List<Object> messages = getMessageList(DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME, runID, "getMessageCountUpTo", null, totalMessages);
        dedupQueueService.sendAll(queueName, messages);
        snooze(1);
        dedupQueueService.sendAll(queueName, messages);
        assertEquals(flexibleDedupQueuePeek(dedupQueueService, queueName, totalMessages).size(), totalMessages);
        assertTrue(dedupQueueService.getMessageCountUpTo(queueName, 1) >= 1, "getMessageCountUpTo should estimate >= " + totalMessages + " docs");
        assertEquals(dedupQueueService.getMessageCountUpTo(queueName, 1500), totalMessages, "Higher limit should find " + totalMessages + " messages");
    }

    @Test
    public void testQueuePeek() {
        String queueName = getQueueName("dedup_queue", "peek", DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME);

        List<Message> preUpdate = flexibleDedupQueuePeek(dedupQueueService, queueName, 5);
        assertEquals(preUpdate.size(), 0, "Pre updates, peek size should be 0");

        List<Object> messages = getMessageList(DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME, runID, "peek", null, 10);
        dedupQueueService.sendAll(queueName, messages);
        snooze(1);
        dedupQueueService.sendAll(queueName, messages);

        assertEquals(flexibleDedupQueuePeek(dedupQueueService, queueName, 5).size(), 5, "Peek should limit to 5 messages");
        assertEquals(flexibleDedupQueuePeek(dedupQueueService, queueName, 15).size(), 10, "Peek should return all 10 messages");
    }

    @Test
    public void testPoll() {
        String queueName = getQueueName("dedup_queue", "poll", DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME);

        List<Message> preUpdate = flexibleDedupQueuePeek(dedupQueueService, queueName, 5);
        assertEquals(preUpdate.size(), 0, "Pre updates, peek size should be 0");

        List<Object> messages = getMessageList(DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME, runID, "peek", null, 10);
        dedupQueueService.sendAll(queueName, messages);
        snooze(1);
        dedupQueueService.sendAll(queueName, messages);

        dedupQueueService.poll(queueName, Duration.ofSeconds(10), 1);
        assertEquals(dedupQueueService.getClaimCount(queueName), 1, "There should be 1 claimed message for 30s");
        snooze(16); //Wait for claim to expire
        assertEquals(dedupQueueService.getClaimCount(queueName), 0, "There should be no claimed message.");

        assertEquals(flexibleDedupQueuePeek(dedupQueueService, queueName, 15).size(), 10, "Peek should return all 10 messages");
        List<Message> queuePollList = dedupQueueService.poll(queueName, Duration.ofMinutes(5), 5);
        assertEquals(queuePollList.size(), 5, "Poll should return 5 messages");
        assertTrue(messages.containsAll(queuePollList.stream().map(Message::getPayload).collect(Collectors.toList())));
    }

    @Test
    public void testAck() {
        String queueName = getQueueName("dedup_queue", "ack", DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME);

        List<Message> preUpdate = flexibleDedupQueuePeek(dedupQueueService, queueName, 5);
        assertEquals(preUpdate.size(), 0, "Pre updates, peek size should be 0");

        List<Object> messages = getMessageList(DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME, runID, "peek", null, 10);
        dedupQueueService.sendAll(queueName, messages);
        snooze(1);
        dedupQueueService.sendAll(queueName, messages);

        List<Message> queuePollList = dedupQueueService.poll(queueName, Duration.ofMinutes(5), 5);
        assertEquals(queuePollList.size(), 5, "Poll should return 5 messages");
        assertEquals(dedupQueueService.getClaimCount(queueName), 5, "Poll should have claimed 5 of 10 messages");
        assertEquals(flexibleDedupQueuePeek(dedupQueueService, queueName, 15).size(), 10, "Peek should return all unacknowledged messages");
        dedupQueueService.acknowledge(queueName, queuePollList.stream().map(Message::getId).collect(Collectors.toList()));
        assertEquals(flexibleDedupQueuePeek(dedupQueueService, queueName, 15).size(), 5, "Peek should return all unacknowledged messages");
        messages.removeAll(queuePollList.stream().map(Message::getPayload).collect(Collectors.toList()));
        snooze(10);
        queuePollList = dedupQueueService.poll(queueName, Duration.ofMinutes(5), 15);
        assertEquals(queuePollList.size(), 5, "Poll should return 5 remaining messages");
        assertEquals(dedupQueueService.getClaimCount(queueName), 5, "Poll should have claimed 5");
        dedupQueueService.acknowledge(queueName, queuePollList.stream().map(Message::getId).collect(Collectors.toList()));
        assertEquals(flexibleDedupQueuePeek(dedupQueueService, queueName, 15).size(), 0, "Peek should  not return any messages");
        messages.removeAll(queuePollList.stream().map(Message::getPayload).collect(Collectors.toList()));
        assertEquals(messages.size(), 0, "Did not get all messages added to queue while polling");
    }

    @Test
    public void testRenew() {
        String queueName = getQueueName("dedup_queue", "renew", DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME);

        List<Object> messages = getMessageList(DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME, runID, "renew", null, 10);
        dedupQueueService.sendAll(queueName, messages);
        snooze(1);
        dedupQueueService.sendAll(queueName, messages);

        assertEquals(flexibleDedupQueuePeek(dedupQueueService, queueName, 100).size(), 10);
        assertEquals(dedupQueueService.getMessageCount(queueName), 10);
        List<Message> shortDurationMessages = dedupQueueService.poll(queueName, Duration.ofSeconds(30), 3);
        List<Message> LongDurationMessages = dedupQueueService.poll(queueName, Duration.ofSeconds(60), 3);
        assertEquals(dedupQueueService.getClaimCount(queueName), 6);
        snooze(45);
        assertEquals(dedupQueueService.getClaimCount(queueName), 3);

        dedupQueueService.renew(queueName, shortDurationMessages.stream().map(Message::getId).collect(Collectors.toList()), Duration.ofMinutes(2));
        assertEquals(dedupQueueService.getClaimCount(queueName), 6);
        snooze(30);
        assertEquals(dedupQueueService.getClaimCount(queueName), 3);

    }

    @Test
    public void testMove() {
        String sourceQueueName = getQueueName("queue_source", "move", DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME);
        String destinationQueueName = getQueueName("queue_destination", "move", DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME);

        String emptyMoveReference = dedupQueueService.moveAsync(sourceQueueName, destinationQueueName);

        while (dedupQueueService.getMoveStatus(emptyMoveReference).getStatus() != COMPLETE) {
            LOGGER.info("Waiting for move to finish");
            snooze(1);
        }

        assertEquals(dedupQueueService.getMessageCount(destinationQueueName), 0, "Empty move should not create messages");
        // Regular move
        List<Object> messages = getMessageList(DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME, runID, "move", null, 10);
        dedupQueueService.sendAll(sourceQueueName, messages);
        snooze(1);
        dedupQueueService.sendAll(sourceQueueName, messages);

        assertEquals(flexibleDedupQueuePeek(dedupQueueService, sourceQueueName, 100).size(), 10);
        assertEquals(dedupQueueService.getMessageCount(sourceQueueName), 10);

        String moveReferance = dedupQueueService.moveAsync(sourceQueueName, destinationQueueName);
        while (dedupQueueService.getMoveStatus(moveReferance).getStatus() != COMPLETE) {
            LOGGER.info("Waiting for move to finish");
            snooze(1);
        }
        LOGGER.info("Move: {}", dedupQueueService.getMoveStatus(moveReferance).getStatus().toString());
        snooze(16);
        assertEquals(dedupQueueService.getMessageCount(sourceQueueName), 0, "sourceQueue should be empty");

        List<Message> pollList = dedupQueueService.poll(destinationQueueName, Duration.ofMinutes(5), 100);
        List<Object> pollObject = pollList.stream().map(Message::getPayload).collect(Collectors.toList());
        pollObject.removeAll(messages);
        assertEquals(pollObject.size(), 0);
        dedupQueueService.acknowledge(destinationQueueName, pollList.stream().map(Message::getId).collect(Collectors.toList()));
        snooze(16);
        assertEquals(dedupQueueService.getMessageCount(destinationQueueName), 0, "destinationQueue should have 10 msgs");

        // Move after claim on source
        List<Object> claimMoveMessages = getMessageList(DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME, runID, "move", Arrays.asList("condition", "claim_move"), 10);
        dedupQueueService.sendAll(sourceQueueName, claimMoveMessages);
        snooze(1);
        dedupQueueService.sendAll(sourceQueueName, claimMoveMessages);
        snooze(1);
        dedupQueueService.poll(sourceQueueName, Duration.ofMinutes(5), 10);
        assertEquals(dedupQueueService.getMessageCount(sourceQueueName), 10, "There should be 10 msgs in source queue");
        assertEquals(dedupQueueService.getClaimCount(sourceQueueName), 10, "All messages should be claimed");

        moveReferance = dedupQueueService.moveAsync(sourceQueueName, destinationQueueName);
        while (dedupQueueService.getMoveStatus(moveReferance).getStatus() != COMPLETE) {
            LOGGER.info("Waiting for move to finish");
            snooze(1);
        }
        snooze(15);
        LOGGER.info("Claim Move: {}", dedupQueueService.getMoveStatus(moveReferance).getStatus().toString());
        assertEquals(dedupQueueService.getClaimCount(destinationQueueName), 0, "Copied messages should not be claimed");
        assertEquals(dedupQueueService.getMessageCount(destinationQueueName), 10, "Copied messages should not be claimed");
        dedupQueueService.acknowledge(destinationQueueName,
                dedupQueueService.poll(destinationQueueName, Duration.ofMinutes(5), 100).stream().map(Message::getId).collect(Collectors.toList())
        );
        // Move after ack on source
        List<Object> ackMoveMessages = getMessageList(DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME, runID, "move", Arrays.asList("condition", "ack_move"), 10);
        dedupQueueService.sendAll(sourceQueueName, ackMoveMessages);
        snooze(1);
        dedupQueueService.sendAll(sourceQueueName, ackMoveMessages);
        snooze(2);
        List<Message> ackPollList = dedupQueueService.poll(sourceQueueName, Duration.ofMinutes(5), 10);
        assertEquals(dedupQueueService.getMessageCount(sourceQueueName), 10, "There should be 10 msgs in source queue");
        dedupQueueService.acknowledge(sourceQueueName, ackPollList.stream().map(Message::getId).collect(Collectors.toList()));
        snooze(16);
        assertEquals(flexibleDedupQueuePeek(dedupQueueService, sourceQueueName).size(), 0, "Should not have any msgs after ack");

        moveReferance = dedupQueueService.moveAsync(sourceQueueName, destinationQueueName);
        while (dedupQueueService.getMoveStatus(moveReferance).getStatus() != COMPLETE) {
            LOGGER.info("Waiting for move to finish");
            snooze(1);
        }
        snooze(16);// wait at least 15 seconds for size cache to expire
        LOGGER.info("Ack Move: {}", dedupQueueService.getMoveStatus(moveReferance).getStatus().toString());
        assertEquals(flexibleDedupQueuePeek(dedupQueueService, destinationQueueName).size(), 0, "No ack messages should not be moved");
    }

    @Test
    public void testDedupQueueUnclaim() {
        String queueName = getQueueName("dedup_queue", "unclaim", DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME);

        List<Object> messages = getMessageList(DEDUP_QUEUE_SERVICE_TEST_CLIENT_NAME, runID, "unclaim", null, 10);
        dedupQueueService.sendAll(queueName, messages);
        snooze(1);
        dedupQueueService.sendAll(queueName, messages);
        assertEquals(flexibleDedupQueuePeek(dedupQueueService, queueName, 10).size(), 10);
        assertEquals(dedupQueueService.getMessageCount(queueName), 10);

        assertEquals(dedupQueueService.poll(queueName, Duration.ofMinutes(5), 2).size(), 2);
        assertEquals(dedupQueueService.getClaimCount(queueName), 2);

        assertEquals(dedupQueueService.poll(queueName, Duration.ofMinutes(5), 5).size(), 5);
        assertEquals(dedupQueueService.getClaimCount(queueName), 7);

        dedupQueueService.unclaimAll(queueName);
        assertEquals(dedupQueueService.getClaimCount(queueName), 0);
        assertEquals(dedupQueueService.getMessageCount(queueName), 10);

        snooze(16);
        dedupQueueService.acknowledge(queueName, dedupQueueService.poll(queueName, Duration.ofMinutes(5), 10).stream().map(Message::getId).collect(Collectors.toList()));
        assertEquals(dedupQueueService.getMessageCount(queueName), 0);
    }

    private static List<Message> flexibleDedupQueuePeek(DedupQueueService dedupQueueService, String queueName) {
        return flexibleDedupQueuePeek(dedupQueueService, queueName, 100);
    }

    private static List<Message> flexibleDedupQueuePeek(DedupQueueService dedupQueueService, String queueName, int limit) {
        // Be flexible with the amount of asynchronous time it takes for EmoDB to add the update events to the databus
        Stopwatch peekTimer = Stopwatch.createStarted();
        List<Message> peekCount;
        while ((peekCount = dedupQueueService.peek(queueName, limit)).size() == 0 && peekTimer.elapsed(TimeUnit.SECONDS) < 20) {
            snooze(8);
        }
        return peekCount;
    }

    private String getQueueName(String base, String apiTested, String clientName) {
        String queueName = uniqueName(base, apiTested, clientName, runID);
        queuesToCleanup.add(queueName);

        LOGGER.info("Name: {}", queueName);
        return queueName;
    }

    private void purgeQueue(String name) {
        try {
            LOGGER.debug("Purging dedup queue: {}", name);
            dedupQueueService.purge(name);
            long queueMsgCount = dedupQueueService.getMessageCount(name);
            int queueCounter = 0;
            while (queueMsgCount > 0 && queueCounter < 10) {
                snooze(2);
                queueCounter++;
            }
            if (dedupQueueService.getMessageCount(name) > 0) {
                LOGGER.warn("Error purging dedup queue {}", name);
            }
        } catch (Exception e) {
            LOGGER.warn("Error purging dedup queue {}", name);
        }
    }
}
