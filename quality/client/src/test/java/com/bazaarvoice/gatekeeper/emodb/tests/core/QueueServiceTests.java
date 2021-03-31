package com.bazaarvoice.gatekeeper.emodb.tests.core;

import com.bazaarvoice.emodb.queue.api.Message;
import com.bazaarvoice.emodb.queue.api.QueueService;
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

@Test(groups = {"emodb.core.all", "emodb.core.queueservice", "queueservice"}, timeOut = 360000)
@Guice(moduleFactory = TestModuleFactory.class)
public class QueueServiceTests {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueueServiceTests.class);
    private static final String QUEUE_SERVICE_TEST_CLIENT_NAME = "gatekeeper_queue_client";

    @Inject
    private QueueService queueService;

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
    public void testSend() {
        final String queueName = getQueueName("queue", "send", QUEUE_SERVICE_TEST_CLIENT_NAME);

        // Send message
        final List<Object> messages = getMessageList(QUEUE_SERVICE_TEST_CLIENT_NAME, runID, "send", null, 1);
        Map<Object, Object> message = (Map<Object, Object>) messages.get(0);
        queueService.send(queueName, message);

        // Verify message
        assertEquals(queueService.getMessageCount(queueName), 1, "Expected 1 message in queue");
        List<Message> peekList = flexibleQueuePeek(queueService, queueName, 10);
        assertEquals(peekList.size(), 1, "Expected 1 message returned from peek");
        assertEquals(peekList.get(0).getPayload(), message, "Found mismatch on sent message.");
    }

    @Test
    public void testSendAll_multipleMessages() {
        final String queueName = getQueueName("multiple_messages", "send_all", QUEUE_SERVICE_TEST_CLIENT_NAME);
        List<Object> messages = getMessageList(QUEUE_SERVICE_TEST_CLIENT_NAME, runID, "send_all", Arrays.asList("condition", "multiple_msgs"), 10);
        queueService.sendAll(queueName, messages);

        // Verify message
        assertEquals(queueService.getMessageCount(queueName), 10, "Expected 1 message in queue");
        List<Object> actualMessageList = flexibleQueuePeek(queueService, queueName, 10)
                .stream().map(Message::getPayload).collect(Collectors.toList());
        assertEquals(actualMessageList, messages);
    }

    @Test
    public void testSendAll_multipleQueues() {
        final String queueOneName = getQueueName("multiple_queues_1", "send_all", QUEUE_SERVICE_TEST_CLIENT_NAME);
        final String queueTwoName = getQueueName("multiple_queues_2", "send_all", QUEUE_SERVICE_TEST_CLIENT_NAME);
        final String queueThreeName = getQueueName("multiple_queues_3", "send_all", QUEUE_SERVICE_TEST_CLIENT_NAME);

        List<Object> expected1 = getMessageList(QUEUE_SERVICE_TEST_CLIENT_NAME, runID, "send_all",
                Arrays.asList("condition", "multiple_queues", "queue", "1"), 1);
        List<Object> expected2 = getMessageList(QUEUE_SERVICE_TEST_CLIENT_NAME, runID, "send_all",
                Arrays.asList("condition", "multiple_queues", "queue", "2"), 2);
        List<Object> expected3 = getMessageList(QUEUE_SERVICE_TEST_CLIENT_NAME, runID, "send_all",
                Arrays.asList("condition", "multiple_queues", "queue", "3"), 3);

        Map<String, List<Object>> combinedJson = ImmutableMap.<String, List<Object>>builder()
                .put(queueOneName, expected1)
                .put(queueTwoName, expected2)
                .put(queueThreeName, expected3)
                .build();

        queueService.sendAll(combinedJson);

        assertEquals(queueService.getMessageCount(queueOneName), 1);
        List<Object> actual1 = flexibleQueuePeek(queueService, queueOneName, 10).stream()
                .map(Message::getPayload)
                .collect(Collectors.toList());
        assertEquals(actual1, expected1);

        assertEquals(queueService.getMessageCount(queueTwoName), 2);
        List<Object> actual2 = flexibleQueuePeek(queueService, queueTwoName, 10).stream()
                .map(Message::getPayload)
                .collect(Collectors.toList());
        assertEquals(actual2, expected2);

        assertEquals(queueService.getMessageCount(queueThreeName), 3);
        List<Object> actual3 = flexibleQueuePeek(queueService, queueThreeName, 10).stream()
                .map(Message::getPayload)
                .collect(Collectors.toList());
        assertEquals(actual3, expected3);
    }

    @Test
    public void testGetMessageCountUpTo() {
        final String queueName = getQueueName("queue", "get_message_count_up_to", QUEUE_SERVICE_TEST_CLIENT_NAME);
        final int totalMessages = 1001;

        List<Object> messages = getMessageList(QUEUE_SERVICE_TEST_CLIENT_NAME, runID, "getMessageCountUpTo", null, totalMessages);
        queueService.sendAll(queueName, messages);

        assertEquals(queueService.getMessageCount(queueName), totalMessages);
        assertTrue(queueService.getMessageCountUpTo(queueName, 1) >= 1, "getMessageCountUpTo should estimate >= " + totalMessages + " docs");
        assertEquals(queueService.getMessageCountUpTo(queueName, 1500), totalMessages, "Higher limit should find " + totalMessages + " messages");
    }

    @Test
    public void testQueuePeek() {
        String queueName = getQueueName("queue", "peek", QUEUE_SERVICE_TEST_CLIENT_NAME);

        List<Message> preUpdate = flexibleQueuePeek(queueService, queueName, 5);
        assertEquals(preUpdate.size(), 0, "Pre updates, peek size should be 0");

        List<Object> messages = getMessageList(QUEUE_SERVICE_TEST_CLIENT_NAME, runID, "peek", null, 10);
        queueService.sendAll(queueName, messages);

        assertEquals(flexibleQueuePeek(queueService, queueName, 5).size(), 5, "Peek should limit to 5 messages");
        assertEquals(flexibleQueuePeek(queueService, queueName, 15).size(), 10, "Peek should return all 10 messages");
    }

    @Test
    public void testPoll() {
        String queueName = getQueueName("queue", "poll", QUEUE_SERVICE_TEST_CLIENT_NAME);

        List<Message> preUpdate = flexibleQueuePeek(queueService, queueName, 5);
        assertEquals(preUpdate.size(), 0, "Pre updates, peek size should be 0");

        List<Object> messages = getMessageList(QUEUE_SERVICE_TEST_CLIENT_NAME, runID, "peek", null, 10);
        queueService.sendAll(queueName, messages);

        queueService.poll(queueName, Duration.ofSeconds(10), 1);
        assertEquals(queueService.getClaimCount(queueName), 1, "There should be 1 claimed message for 30s");
        snooze(16); //Wait for claim to expire
        assertEquals(queueService.getClaimCount(queueName), 0, "There should be no claimed message.");

        assertEquals(flexibleQueuePeek(queueService, queueName, 15).size(), 10, "Peek should return all 10 messages");
        List<Message> queuePollList = queueService.poll(queueName, Duration.ofMinutes(5), 5);
        assertEquals(queuePollList.size(), 5, "Poll should return 5 messages");
        assertTrue(messages.containsAll(queuePollList.stream().map(Message::getPayload).collect(Collectors.toList())));
    }

    @Test
    public void testAck() {
        String queueName = getQueueName("queue", "ack", QUEUE_SERVICE_TEST_CLIENT_NAME);

        List<Message> preUpdate = flexibleQueuePeek(queueService, queueName, 5);
        assertEquals(preUpdate.size(), 0, "Pre updates, peek size should be 0");

        List<Object> messages = getMessageList(QUEUE_SERVICE_TEST_CLIENT_NAME, runID, "peek", null, 10);
        queueService.sendAll(queueName, messages);

        List<Message> queuePollList = queueService.poll(queueName, Duration.ofMinutes(5), 5);
        assertEquals(queuePollList.size(), 5, "Poll should return 5 messages");
        assertEquals(queueService.getClaimCount(queueName), 5, "Poll should have claimed 5 of 10 messages");
        assertEquals(flexibleQueuePeek(queueService, queueName, 15).size(), 10, "Peek should return all unacknowledged messages");
        queueService.acknowledge(queueName, queuePollList.stream().map(Message::getId).collect(Collectors.toList()));
        assertEquals(flexibleQueuePeek(queueService, queueName, 15).size(), 5, "Peek should return all unacknowledged messages");
        messages.removeAll(queuePollList.stream().map(Message::getPayload).collect(Collectors.toList()));
        snooze(10);
        queuePollList = queueService.poll(queueName, Duration.ofMinutes(5), 15);
        assertEquals(queuePollList.size(), 5, "Poll should return 5 remaining messages");
        assertEquals(queueService.getClaimCount(queueName), 5, "Poll should have claimed 5");
        queueService.acknowledge(queueName, queuePollList.stream().map(Message::getId).collect(Collectors.toList()));
        assertEquals(flexibleQueuePeek(queueService, queueName, 15).size(), 0, "Peek should  not return any messages");
        messages.removeAll(queuePollList.stream().map(Message::getPayload).collect(Collectors.toList()));
        assertEquals(messages.size(), 0, "Did not get all messages added to queue while polling");
    }

    @Test
    public void testRenew() {
        String queueName = getQueueName("queue", "renew", QUEUE_SERVICE_TEST_CLIENT_NAME);

        List<Object> messages = getMessageList(QUEUE_SERVICE_TEST_CLIENT_NAME, runID, "renew", null, 10);
        queueService.sendAll(queueName, messages);

        assertEquals(queueService.getMessageCount(queueName), 10);
        List<Message> shortDurationMessages = queueService.poll(queueName, Duration.ofSeconds(30), 3);
        List<Message> LongDurationMessages = queueService.poll(queueName, Duration.ofSeconds(60), 3);
        assertEquals(queueService.getClaimCount(queueName), 6);
        snooze(45);
        assertEquals(queueService.getClaimCount(queueName), 3);

        queueService.renew(queueName, shortDurationMessages.stream().map(Message::getId).collect(Collectors.toList()), Duration.ofMinutes(2));
        assertEquals(queueService.getClaimCount(queueName), 6);
        snooze(30);
        assertEquals(queueService.getClaimCount(queueName), 3);
    }

    @Test
    public void testMove() {
        String sourceQueueName = getQueueName("queue_source", "move", QUEUE_SERVICE_TEST_CLIENT_NAME);
        String destinationQueueName = getQueueName("queue_destination", "move", QUEUE_SERVICE_TEST_CLIENT_NAME);

        queueService.moveAsync(sourceQueueName, destinationQueueName);
        assertEquals(queueService.getMessageCount(destinationQueueName), 0, "Empty move should not create messages");

        // Regular move
        List<Object> messages = getMessageList(QUEUE_SERVICE_TEST_CLIENT_NAME, runID, "move", null, 10);
        queueService.sendAll(sourceQueueName, messages);
        assertEquals(queueService.getMessageCount(sourceQueueName), 10);

        String moveReferance = queueService.moveAsync(sourceQueueName, destinationQueueName);
        while (queueService.getMoveStatus(moveReferance).getStatus() != COMPLETE) {
            LOGGER.info("Waiting for move to finish");
            snooze(1);
        }
        LOGGER.info("Move: {}", queueService.getMoveStatus(moveReferance).getStatus().toString());
        snooze(16);
        assertEquals(queueService.getMessageCount(sourceQueueName), 0, "sourceQueue should be empty");

        List<Message> pollList = queueService.poll(destinationQueueName, Duration.ofMinutes(5), 100);
        List<Object> pollObject = pollList.stream().map(Message::getPayload).collect(Collectors.toList());
        pollObject.removeAll(messages);
        assertEquals(pollObject.size(), 0);
        queueService.acknowledge(destinationQueueName, pollList.stream().map(Message::getId).collect(Collectors.toList()));
        snooze(16);
        assertEquals(queueService.getMessageCount(destinationQueueName), 0, "destinationQueue should have 10 msgs");

        // Move after claim on source
        List<Object> claimMoveMessages = getMessageList(QUEUE_SERVICE_TEST_CLIENT_NAME, runID, "move", Arrays.asList("condition", "claim_move"), 10);
        queueService.sendAll(sourceQueueName, claimMoveMessages);
        snooze(5);
        assertEquals(queueService.getMessageCount(sourceQueueName), 10, "There should be 10 msgs in source queue");
        queueService.poll(sourceQueueName, Duration.ofMinutes(5), 10);
        assertEquals(queueService.getClaimCount(sourceQueueName), 10, "All messages should be claimed");

        moveReferance = queueService.moveAsync(sourceQueueName, destinationQueueName);
        while (queueService.getMoveStatus(moveReferance).getStatus() != COMPLETE) {
            LOGGER.info("Waiting for move to finish");
            snooze(1);
        }
        snooze(15);
        LOGGER.info("Claim Move: {}", queueService.getMoveStatus(moveReferance).getStatus().toString());
        assertEquals(queueService.getClaimCount(destinationQueueName), 0, "Copied messages should not be claimed");
        assertEquals(queueService.getMessageCount(destinationQueueName), 10, "Copied messages should not be claimed");
        queueService.acknowledge(destinationQueueName,
                queueService.poll(destinationQueueName, Duration.ofMinutes(5), 100).stream().map(Message::getId).collect(Collectors.toList())
        );

        // Move after ack on source
        List<Object> ackMoveMessages = getMessageList(QUEUE_SERVICE_TEST_CLIENT_NAME, runID, "move", Arrays.asList("condition", "ack_move"), 10);
        queueService.sendAll(sourceQueueName, ackMoveMessages);
        snooze(5);
        assertEquals(queueService.getMessageCount(sourceQueueName), 10, "There should be 10 msgs in source queue");
        List<Message> ackPollList = queueService.poll(sourceQueueName, Duration.ofMinutes(5), 10);
        queueService.acknowledge(sourceQueueName, ackPollList.stream().map(Message::getId).collect(Collectors.toList()));
        snooze(16);
        assertEquals(flexibleQueuePeek(queueService, sourceQueueName).size(), 0, "Should not have any msgs after ack");

        moveReferance = queueService.moveAsync(sourceQueueName, destinationQueueName);
        while (queueService.getMoveStatus(moveReferance).getStatus() != COMPLETE) {
            LOGGER.info("Waiting for move to finish");
            snooze(1);
        }
        snooze(16);// wait at least 15 seconds for size cache to expire
        LOGGER.info("Ack Move: {}", queueService.getMoveStatus(moveReferance).getStatus().toString());
        assertEquals(flexibleQueuePeek(queueService, destinationQueueName).size(), 0, "No ack messages should not be moved");
    }

    @Test
    public void testQueueUnclaim() {
        String queueName = getQueueName("queue", "unclaim", QUEUE_SERVICE_TEST_CLIENT_NAME);

        List<Object> messages = getMessageList(QUEUE_SERVICE_TEST_CLIENT_NAME, runID, "unclaim", null, 10);
        queueService.sendAll(queueName, messages);
        assertEquals(queueService.getMessageCount(queueName), 10);

        assertEquals(queueService.poll(queueName, Duration.ofMinutes(5), 2).size(), 2);
        assertEquals(queueService.getClaimCount(queueName), 2);

        assertEquals(queueService.poll(queueName, Duration.ofMinutes(5), 5).size(), 5);
        assertEquals(queueService.getClaimCount(queueName), 7);

        queueService.unclaimAll(queueName);
        assertEquals(queueService.getClaimCount(queueName), 0);
        assertEquals(queueService.getMessageCount(queueName), 10);

        snooze(16);
        queueService.acknowledge(queueName, queueService.poll(queueName, Duration.ofMinutes(5), 10).stream().map(Message::getId).collect(Collectors.toList()));
        assertEquals(queueService.getMessageCount(queueName), 0);
    }

    private static List<Message> flexibleQueuePeek(QueueService queueService, String queueName) {
        return flexibleQueuePeek(queueService, queueName, 100);
    }

    private static List<Message> flexibleQueuePeek(QueueService queueService, String queueName, int limit) {
        // Be flexible with the amount of asynchronous time it takes for EmoDB to add the update events to the databus
        Stopwatch peekTimer = Stopwatch.createStarted();
        List<Message> peekCount;
        while ((peekCount = queueService.peek(queueName, limit)).size() == 0 && peekTimer.elapsed(TimeUnit.SECONDS) < 20) {
            snooze(8);
        }
        return peekCount;
    }

    private String getQueueName(String base, String apiTested, String clientName) {
        String queueName = uniqueName(base, apiTested, clientName, runID);
        queuesToCleanup.add(queueName);
        LOGGER.info("Queue: {}", queueName);
        return queueName;
    }

    private void purgeQueue(String name) {
        try {
            LOGGER.info("Purging: {}", name);
            queueService.purge(name);
            long queueMsgCount = queueService.getMessageCount(name);
            int queueCounter = 0;
            while (queueMsgCount > 0 && queueCounter < 10) {
                snooze(2);
                queueCounter++;
            }
            if (queueService.getMessageCount(name) > 0) {
                LOGGER.warn("Error purging queue {}", name);
            }
        } catch (Exception e) {
            LOGGER.warn("Error purging queue {}", name);
        }
    }
}
