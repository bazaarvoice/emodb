package test.integration.queue;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.common.api.ServiceUnavailableException;
import com.bazaarvoice.emodb.common.api.UnauthorizedException;
import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import com.bazaarvoice.emodb.queue.api.AuthQueueService;
import com.bazaarvoice.emodb.queue.api.Message;
import com.bazaarvoice.emodb.queue.api.MoveQueueStatus;
import com.bazaarvoice.emodb.queue.api.QueueService;
import com.bazaarvoice.emodb.queue.client.QueueClient;
import com.bazaarvoice.emodb.queue.client.QueueServiceAuthenticator;
import com.bazaarvoice.emodb.test.ResourceTest;
import com.bazaarvoice.emodb.web.partition.PartitionForwardingException;
import com.bazaarvoice.emodb.web.resources.queue.QueueResource1;
import com.bazaarvoice.ostrich.PartitionContextBuilder;
import com.bazaarvoice.ostrich.pool.OstrichAccessors;
import com.bazaarvoice.ostrich.pool.PartitionContextValidator;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.apache.http.conn.ConnectTimeoutException;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Tests the api calls made via the Jersey HTTP client {@link QueueClient} are
 * interpreted correctly by the Jersey HTTP resource {@link QueueResource1}.
 */
public class QueueJerseyTest extends ResourceTest {
    private static final String APIKEY_QUEUE = "queue-key";
    private static final String APIKEY_UNAUTHORIZED = "unauthorized-key";

    private final PartitionContextValidator<AuthQueueService> _pcxtv =
            OstrichAccessors.newPartitionContextTest(AuthQueueService.class, QueueClient.class);
    private final QueueService _server = mock(QueueService.class);
    private final AuthQueueService _proxy = mock(AuthQueueService.class);

    @Rule
    public ResourceTestRule _resourceTestRule = setupResourceTestRule(
            Collections.<Object>singletonList(new QueueResource1(_server, QueueServiceAuthenticator.proxied(_proxy), new MetricRegistry())),
            ImmutableMap.of(
                    APIKEY_QUEUE, new ApiKey("queue", ImmutableSet.of("queue-role")),
                    APIKEY_UNAUTHORIZED, new ApiKey("unauth", ImmutableSet.of("unauthorized-role"))),
            ImmutableMultimap.of("queue-role", "queue|*|*"));

    @After
    public void tearDownMocksAndClearState() {
        verifyNoMoreInteractions(_server, _proxy);
        reset(_server, _proxy);
    }

    private QueueService queueClient() {
        return QueueServiceAuthenticator.proxied(new QueueClient(URI.create("/queue/1"), new JerseyEmoClient(_resourceTestRule.client())))
                .usingCredentials(APIKEY_QUEUE);
    }

    private QueueService queueClient(boolean partitioned) {
        return QueueServiceAuthenticator.proxied(new QueueClient(URI.create("/queue/1"), partitioned, new JerseyEmoClient(_resourceTestRule.client())))
                .usingCredentials(APIKEY_QUEUE);
    }

    private QueueService unauthorizedQueueClient(boolean partitioned) {
        return QueueServiceAuthenticator.proxied(new QueueClient(URI.create("/queue/1"), partitioned, new JerseyEmoClient(_resourceTestRule.client())))
                .usingCredentials(APIKEY_UNAUTHORIZED);
    }

    @Test
    public void testSendPartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.empty())
                .send(APIKEY_QUEUE, "queue-name", "json-message");
    }

    @Test
    public void testSend() {
        queueClient().send("queue-name", "json-message");

        verify(_server).send("queue-name", "json-message");
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testSendAllEmpty() {
        queueClient().sendAll("queue-name", ImmutableList.of());

        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testSendAllPartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.empty())
                .sendAll(APIKEY_QUEUE, "queue-name", ImmutableList.of("json-message", 1234));
    }

    @Test
    public void testSendAll() {
        queueClient().sendAll("queue-name", ImmutableList.of("json-message", 1234));

        verify(_server).sendAll("queue-name", ImmutableList.of("json-message", 1234));
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testSendAllWithEmptyMap() {
        queueClient().sendAll(ImmutableMap.<String, List<Object>>of());

        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testSendAllWithSingleQueueMap() {
        List<Object> messages = ImmutableList.<Object>of("json-message", 1234);

        queueClient().sendAll(ImmutableMap.of("queue-name", messages));

        verify(_server).sendAll("queue-name", messages);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testSendAllWithMapPartitionContext() {
        Map<String, List<Object>> messages = ImmutableMap.<String, List<Object>>of(
                "queue1", ImmutableList.<Object>of("json-message", 1234),
                "queue2", ImmutableList.<Object>of(true, ImmutableMap.of("key", "value")));

        _pcxtv.expect(PartitionContextBuilder.empty())
                .sendAll(APIKEY_QUEUE, messages);
    }

    @Test
    public void testSendAllWithMap() {
        Map<String, List<Object>> messages = ImmutableMap.<String, List<Object>>of(
                "queue1", ImmutableList.<Object>of("json-message", 1234),
                "queue2", ImmutableList.<Object>of(true, ImmutableMap.of("key", "value")));

        queueClient().sendAll(messages);

        verify(_server).sendAll(messages);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetMessageCountPartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.empty())
                .getMessageCount(APIKEY_QUEUE, "queue-name");
    }

    @Test
    public void testGetMessageCount() {
        long expected = 123L;
        when(_server.getMessageCount("queue-name")).thenReturn(expected);

        long actual = queueClient().getMessageCount("queue-name");

        assertEquals(actual, expected);
        verify(_server).getMessageCount("queue-name");
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetMessageCountUpToPartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.empty())
                .getMessageCountUpTo(APIKEY_QUEUE, "queue-name", 10000L);
    }

    @Test
    public void testGetMessageCountUpTo() {
        long expected = 123L;
        when(_server.getMessageCountUpTo("queue-name", 10000L)).thenReturn(expected);

        long actual = queueClient().getMessageCountUpTo("queue-name", 10000L);

        assertEquals(actual, expected);
        verify(_server).getMessageCountUpTo("queue-name", 10000L);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetClaimCountPartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.of("queue-name"))
                .getClaimCount(APIKEY_QUEUE, "queue-name");
    }

    @Test
    public void testGetClaimCount() {
        long expected = 123L;
        when(_proxy.getClaimCount(APIKEY_QUEUE, "queue-name")).thenReturn(expected);

        long actual = queueClient().getClaimCount("queue-name");

        assertEquals(actual, expected);
        verify(_proxy).getClaimCount(APIKEY_QUEUE, "queue-name");
        verifyNoMoreInteractions(_proxy);
    }

    @Test
    public void testGetClaimCountPartitioned() {
        long expected = 123L;
        when(_server.getClaimCount("queue-name")).thenReturn(expected);

        long actual = queueClient(true).getClaimCount("queue-name");

        assertEquals(actual, expected);
        verify(_server).getClaimCount("queue-name");
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testPeekPartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.empty())
                .peek(APIKEY_QUEUE, "queue-name", 123);
    }

    @Test
    public void testPeek() {
        List<Message> expected = ImmutableList.of(new Message("id-1", "payload-1"), new Message("id-2", "payload-2"));
        when(_server.peek("queue-name", 123)).thenReturn(expected);

        List<Message> actual = queueClient().peek("queue-name", 123);

        assertEquals(actual, expected);
        verify(_server).peek("queue-name", 123);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testPollPartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.of("queue-name"))
                .poll(APIKEY_QUEUE, "queue-name", Duration.ofSeconds(15), 123);
    }

    @Test
    public void testPoll() {
        List<Message> expected = ImmutableList.of(new Message("id-1", "payload-1"), new Message("id-2", "payload-2"));
        when(_proxy.poll(APIKEY_QUEUE, "queue-name", Duration.ofSeconds(15), 123)).thenReturn(expected);

        List<Message> actual = queueClient().poll("queue-name", Duration.ofSeconds(15), 123);

        assertEquals(actual, expected);
        verify(_proxy).poll(APIKEY_QUEUE, "queue-name", Duration.ofSeconds(15), 123);
        verifyNoMoreInteractions(_proxy);
    }


    @Test
    public void testPollUnauthorized() {
        try {
            unauthorizedQueueClient(false).poll("queue-name", Duration.ofSeconds(15), 123);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof UnauthorizedException);
        }

        verifyNoMoreInteractions(_proxy);
    }

    @Test
    public void testPollPartitioned() {
        List<Message> expected = ImmutableList.of(new Message("id-1", "payload-1"), new Message("id-2", "payload-2"));
        when(_server.poll("queue-name", Duration.ofSeconds(15), 123)).thenReturn(expected);

        List<Message> actual = queueClient(true).poll("queue-name", Duration.ofSeconds(15), 123);

        assertEquals(actual, expected);
        verify(_server).poll("queue-name", Duration.ofSeconds(15), 123);
        verifyNoMoreInteractions(_server);
    }


    @Test
    public void testPollUnauthorizedPartitioned() {
        try {
            unauthorizedQueueClient(true).poll("queue-name", Duration.ofSeconds(15), 123);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof UnauthorizedException);
        }

        verifyNoMoreInteractions(_proxy);
    }

    @Test
    public void testPollPartitionTimeout() {
        when(_proxy.poll(APIKEY_QUEUE, "queue-name", Duration.ofSeconds(15), 123))
                .thenThrow(new PartitionForwardingException(new ConnectTimeoutException()));

        try {
            queueClient().poll("queue-name", Duration.ofSeconds(15), 123);
        } catch (ServiceUnavailableException e) {
            // Ok
        }

        verify(_proxy).poll(APIKEY_QUEUE, "queue-name", Duration.ofSeconds(15), 123);
        verifyNoMoreInteractions(_proxy);
    }

    @Test
    public void testRenewPartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.of("queue-name"))
                .renew(APIKEY_QUEUE, "queue-name", ImmutableList.of("id-1", "id-2"), Duration.ofSeconds(15));
    }

    @Test
    public void testRenew() {
        queueClient().renew("queue-name", ImmutableList.of("id-1", "id-2"), Duration.ofSeconds(15));

        verify(_proxy).renew(APIKEY_QUEUE, "queue-name", ImmutableList.of("id-1", "id-2"), Duration.ofSeconds(15));
        verifyNoMoreInteractions(_proxy);
    }

    @Test
    public void testRenewPartitioned() {
        queueClient(true).renew("queue-name", ImmutableList.of("id-1", "id-2"), Duration.ofSeconds(15));

        verify(_server).renew("queue-name", ImmutableList.of("id-1", "id-2"), Duration.ofSeconds(15));
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testAcknowledgePartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.of("queue-name"))
                .acknowledge(APIKEY_QUEUE, "queue-name", ImmutableList.of("id-1", "id-2"));
    }

    @Test
    public void testAcknowledge() {
        queueClient().acknowledge("queue-name", ImmutableList.of("id-1", "id-2"));

        verify(_proxy).acknowledge(APIKEY_QUEUE, "queue-name", ImmutableList.of("id-1", "id-2"));
        verifyNoMoreInteractions(_proxy);
    }

    @Test
    public void testAcknowledgePartitioned() {
        queueClient(true).acknowledge("queue-name", ImmutableList.of("id-1", "id-2"));

        verify(_server).acknowledge("queue-name", ImmutableList.of("id-1", "id-2"));
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testMove() {
        when(_server.moveAsync("queue-src", "queue-dest")).thenReturn("moveId1");

        String moveId = queueClient().moveAsync("queue-src", "queue-dest");

        verify(_server).moveAsync("queue-src", "queue-dest");
        verifyNoMoreInteractions(_server);
        assertEquals(moveId, "moveId1");
    }

    @Test
    public void testMoveStatus() {
        MoveQueueStatus expected = new MoveQueueStatus("queue-src", "queue-dest", MoveQueueStatus.Status.IN_PROGRESS);
        when(_server.getMoveStatus("moveId1")).thenReturn(expected);

        MoveQueueStatus status = queueClient().getMoveStatus("moveId1");

        verify(_server).getMoveStatus("moveId1");
        verifyNoMoreInteractions(_server);
        assertEquals(status.getFrom(), "queue-src");
        assertEquals(status.getTo(), "queue-dest");
        assertEquals(status.getStatus(), MoveQueueStatus.Status.IN_PROGRESS);
    }

    @Test
    public void testUnclaimAllPartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.of("queue-name"))
                .unclaimAll(APIKEY_QUEUE, "queue-name");
    }

    @Test
    public void testUnclaimAll() {
        queueClient().unclaimAll("queue-name");

        verify(_proxy).unclaimAll(APIKEY_QUEUE, "queue-name");
        verifyNoMoreInteractions(_proxy);
    }

    @Test
    public void testUnclaimAllPartitioned() {
        queueClient(true).unclaimAll("queue-name");

        verify(_server).unclaimAll("queue-name");
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testPurgePartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.of("queue-name"))
                .purge(APIKEY_QUEUE, "queue-name");
    }

    @Test
    public void testPurge() {
        queueClient().purge("queue-name");

        verify(_proxy).purge(APIKEY_QUEUE, "queue-name");
        verifyNoMoreInteractions(_proxy);
    }

    @Test
    public void testPurgePartitioned() {
        queueClient(true).purge("queue-name");

        verify(_server).purge("queue-name");
        verifyNoMoreInteractions(_server);
    }
}
