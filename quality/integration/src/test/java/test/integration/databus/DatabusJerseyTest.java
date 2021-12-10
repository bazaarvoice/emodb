package test.integration.databus;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.common.api.ServiceUnavailableException;
import com.bazaarvoice.emodb.common.api.UnauthorizedException;
import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.databus.api.DefaultSubscription;
import com.bazaarvoice.emodb.databus.api.Event;
import com.bazaarvoice.emodb.databus.api.EventViews;
import com.bazaarvoice.emodb.databus.api.MoveSubscriptionStatus;
import com.bazaarvoice.emodb.databus.api.PollResult;
import com.bazaarvoice.emodb.databus.api.ReplaySubscriptionStatus;
import com.bazaarvoice.emodb.databus.api.Subscription;
import com.bazaarvoice.emodb.databus.api.UnauthorizedSubscriptionException;
import com.bazaarvoice.emodb.databus.api.UnknownSubscriptionException;
import com.bazaarvoice.emodb.databus.client.DatabusAuthenticator;
import com.bazaarvoice.emodb.databus.client.DatabusClient;
import com.bazaarvoice.emodb.databus.core.DatabusChannelConfiguration;
import com.bazaarvoice.emodb.databus.core.DatabusEventStore;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.core.UpdateRef;
import com.bazaarvoice.emodb.test.ResourceTest;
import com.bazaarvoice.emodb.web.partition.PartitionForwardingException;
import com.bazaarvoice.emodb.web.resources.databus.AbstractSubjectDatabus;
import com.bazaarvoice.emodb.web.resources.databus.DatabusResource1;
import com.bazaarvoice.emodb.web.resources.databus.DatabusResourcePoller;
import com.bazaarvoice.emodb.web.resources.databus.LongPollingExecutorServices;
import com.bazaarvoice.emodb.web.resources.databus.PeekOrPollResponseHelper;
import com.bazaarvoice.emodb.web.resources.databus.SubjectDatabus;
import com.bazaarvoice.ostrich.PartitionContextBuilder;
import com.bazaarvoice.ostrich.pool.OstrichAccessors;
import com.bazaarvoice.ostrich.pool.PartitionContextValidator;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.spi.inject.SingletonTypeInjectableProvider;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.apache.http.conn.ConnectTimeoutException;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.servlet.AsyncContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Tests the api calls made via the Jersey HTTP client {@link DatabusClient} are
 * interpreted correctly by the Jersey HTTP resource {@link DatabusResource1}.
 */
public class DatabusJerseyTest extends ResourceTest {
    private static final String APIKEY_DATABUS = "databus-key";
    private static final String INTERNAL_ID_DATABUS = "databus-id";
    private static final String APIKEY_UNAUTHORIZED = "unauthorized-key";
    private static final String INTERNAL_ID_UNAUTHORIZED = "unauthorized-id";

    private final PartitionContextValidator<SubjectDatabus> _pcxtv =
            OstrichAccessors.newPartitionContextTest(SubjectDatabus.class, AbstractSubjectDatabus.class);
    private final SubjectDatabus _local = mock(SubjectDatabus.class);
    private final SubjectDatabus _client = mock(SubjectDatabus.class);

    @Rule
    public ResourceTestRule _resourceTestRule = setupResourceTestRule(
            Collections.<Object>singletonList(new DatabusResource1(_local, _client, mock(DatabusEventStore.class),
                    new DatabusResourcePoller(new MetricRegistry()))),
            ImmutableMap.of(
                    APIKEY_DATABUS, new ApiKey(INTERNAL_ID_DATABUS, ImmutableSet.of("databus-role")),
                    APIKEY_UNAUTHORIZED, new ApiKey(INTERNAL_ID_UNAUTHORIZED, ImmutableSet.of("unauthorized-role"))),
            ImmutableMultimap.of("databus-role", "databus|*|*"));

    @After
    public void tearDownMocksAndClearState() {
        verifyNoMoreInteractions(_local, _client);
        reset(_local, _client);
    }

    private Databus databusClient() {
        return DatabusAuthenticator.proxied(new DatabusClient(URI.create("/bus/1"), new JerseyEmoClient(_resourceTestRule.client())))
                .usingCredentials(APIKEY_DATABUS);
    }

    private Databus databusClient(boolean partitioned) {
        return DatabusAuthenticator.proxied(new DatabusClient(URI.create("/bus/1"), partitioned, new JerseyEmoClient(_resourceTestRule.client())))
                .usingCredentials(APIKEY_DATABUS);
    }

    private Databus unauthorizedDatabusClient(boolean partitioned) {
        return DatabusAuthenticator.proxied(new DatabusClient(URI.create("/bus/1"), partitioned, new JerseyEmoClient(_resourceTestRule.client())))
                .usingCredentials(APIKEY_UNAUTHORIZED);
    }

    private ArgumentMatcher<Subject> matchesSubject(final String apiKey, final String id) {
        return new ArgumentMatcher<Subject>() {
            @Override
            public boolean matches(Subject subject) {
                return subject != null && subject.getAuthenticationId().equals(apiKey) && subject.getId().equals(id);
            }

            @Override
            public String toString() {
                return "API key " + apiKey;
            }
        };
    }

    private Subject isSubject() {
        return argThat(matchesSubject(APIKEY_DATABUS, INTERNAL_ID_DATABUS));
    }

    private Subject isUnauthSubject() {
        return argThat(matchesSubject(APIKEY_UNAUTHORIZED, INTERNAL_ID_UNAUTHORIZED));
    }

    private Subject createSubject() {
        Subject subject = mock(Subject.class);
        when(subject.getAuthenticationId()).thenReturn(APIKEY_DATABUS);
        when(subject.getId()).thenReturn(INTERNAL_ID_DATABUS);
        return subject;
    }

    @Test
    public void testListSubscriptions() {
        _pcxtv.expect(PartitionContextBuilder.empty())
                .listSubscriptions(createSubject(), "from-queue", 123L);
    }

    @Test
    public void testListSubscriptions1() {
        when(_local.listSubscriptions(isSubject(), nullable(String.class), eq(Long.MAX_VALUE))).thenReturn(Collections.<Subscription>emptyIterator());

        Iterator<Subscription> actual = databusClient().listSubscriptions(null, Long.MAX_VALUE);

        assertFalse(actual.hasNext());
        verify(_local).listSubscriptions(isSubject(), nullable(String.class), eq(Long.MAX_VALUE));
        verifyNoMoreInteractions(_local);
    }

    @Test
    public void testListSubscriptions2() {
        Date now = new Date();
        List<Subscription> expected = ImmutableList.<Subscription>of(
                new DefaultSubscription("queue-name1", Conditions.alwaysTrue(), now, Duration.ofHours(48)),
                new DefaultSubscription("queue-name2", Conditions.intrinsic(Intrinsic.TABLE, "test"), now, Duration.ofDays(7)));
        when(_local.listSubscriptions(isSubject(), eq("queue-name"), eq(123L))).thenReturn(expected.iterator());

        List<Subscription> actual = ImmutableList.copyOf(
                databusClient().listSubscriptions("queue-name", 123L));

        assertEquals(actual.size(), expected.size());
        for (int i = 0; i < actual.size(); i++) {
            assertSubscriptionEquals(actual.get(i), expected.get(i));
        }
        verify(_local).listSubscriptions(isSubject(), eq("queue-name"), eq(123L));
        verifyNoMoreInteractions(_local);
    }

    @Test
    public void testSubscribePartitionContext() {
        Condition condition = Conditions.intrinsic(Intrinsic.TABLE, "test");
        Duration subscriptionTtl = Duration.ofDays(15);
        Duration eventTtl = Duration.ofDays(2);

        // By default we ignore any events tagged with "re-etl"
        Condition expectedCondition = Conditions.and(condition,
                Conditions.not(Conditions.mapBuilder().matches(UpdateRef.TAGS_NAME, Conditions.containsAny("re-etl")).build()));

        _pcxtv.expect(PartitionContextBuilder.empty())
                .subscribe(createSubject(), "queue-name", expectedCondition, subscriptionTtl, eventTtl);
    }

    @Test
    public void testSubscribePartitionContextWithoutIgnoringAnyEvents() {
        Condition condition = Conditions.intrinsic(Intrinsic.TABLE, "test");
        Duration subscriptionTtl = Duration.ofDays(15);
        Duration eventTtl = Duration.ofDays(2);

        _pcxtv.expect(PartitionContextBuilder.empty())
                .subscribe(createSubject(), "queue-name", condition, subscriptionTtl, eventTtl, false);
    }

    @Test
    public void testSubscribe() {
        Condition condition = Conditions.intrinsic(Intrinsic.TABLE, "test");
        Duration subscriptionTtl = Duration.ofDays(15);
        Duration eventTtl = Duration.ofDays(2);

        databusClient().subscribe("queue-name", condition, subscriptionTtl, eventTtl);

        verify(_local).subscribe(isSubject(), eq("queue-name"), eq(condition), eq(subscriptionTtl), eq(eventTtl), eq(true));
        verifyNoMoreInteractions(_local);
    }

    @Test
    public void testSubscribeWithoutIgnoringAnyEvents() {
        Condition condition = Conditions.intrinsic(Intrinsic.TABLE, "test");
        Duration subscriptionTtl = Duration.ofDays(15);
        Duration eventTtl = Duration.ofDays(2);

        databusClient().subscribe("queue-name", condition, subscriptionTtl, eventTtl, false);

        verify(_local).subscribe(isSubject(), eq("queue-name"), eq(condition), eq(subscriptionTtl), eq(eventTtl), eq(false));
        verifyNoMoreInteractions(_local);
    }

    @Test
    public void testSubscribeNotOwner() {
        Condition condition = Conditions.intrinsic(Intrinsic.TABLE, "test");
        Duration subscriptionTtl = Duration.ofDays(15);
        Duration eventTtl = Duration.ofDays(2);

        doThrow(new UnauthorizedSubscriptionException("Not owner", "queue-name")).
                when(_local).subscribe(isSubject(), eq("queue-name"), eq(condition), eq(subscriptionTtl), eq(eventTtl), eq(true));

        try {
            databusClient().subscribe("queue-name", condition, subscriptionTtl, eventTtl);
            fail();
        } catch (UnauthorizedSubscriptionException e) {
            assertEquals(e.getSubscription(), "queue-name");
        }

        verify(_local).subscribe(isSubject(), eq("queue-name"), eq(condition), eq(subscriptionTtl), eq(eventTtl), eq(true));
        verifyNoMoreInteractions(_local);
    }

    @Test
    public void testUnsubscribePartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.of("queue-name"))
                .unsubscribe(createSubject(), "queue-name");
    }

    @Test
    public void testUnsubscribe() {
        databusClient().unsubscribe("queue-name");

        verify(_client).unsubscribe(isSubject(), eq("queue-name"));
        verifyNoMoreInteractions(_client);
    }

    @Test
    public void testUnsubscribePartitioned() {
        databusClient(true).unsubscribe("queue-name");

        verify(_local).unsubscribe(isSubject(), eq("queue-name"));
        verifyNoMoreInteractions(_local);
    }

    @Test
    public void testGetSubscriptionPartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.empty())
                .getSubscription(createSubject(), "queue-name");
    }

    @Test
    public void testGetSubscription() {
        Date now = new Date();
        Subscription expected = new DefaultSubscription("queue-name", Conditions.alwaysTrue(), now, Duration.ofHours(48));
        when(_local.getSubscription(isSubject(), eq("queue-name"))).thenReturn(expected);

        Subscription actual = databusClient().getSubscription("queue-name");

        assertSubscriptionEquals(expected, actual);
        verify(_local).getSubscription(isSubject(), eq("queue-name"));
        verifyNoMoreInteractions(_local);
    }

    @Test
    public void testGetUnknownSubscription() {
        UnknownSubscriptionException expected = new UnknownSubscriptionException("queue-name");
        when(_local.getSubscription(isSubject(), eq("queue-name"))).thenThrow(expected);

        try {
            databusClient().getSubscription("queue-name");
            fail();
        } catch (UnknownSubscriptionException actual) {
            assertEquals(actual.getSubscription(), "queue-name");
            assertNotSame(actual, expected);  // Should be recreated by exception mappers
        }

        verify(_local).getSubscription(isSubject(), eq("queue-name"));
        verifyNoMoreInteractions(_local);
    }

    @Test
    public void testGetEventCountPartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.of("queue-name"))
                .getEventCount(createSubject(), "queue-name");
    }

    @Test
    public void testGetEventCount() {
        long expected = 123L;
        when(_client.getEventCount(isSubject(), eq("queue-name"))).thenReturn(expected);

        long actual = databusClient().getEventCount("queue-name");

        assertEquals(actual, expected);
        verify(_client).getEventCount(isSubject(), eq("queue-name"));
        verifyNoMoreInteractions(_local, _client);
    }

    @Test
    public void testGetEventCountPartitioned() {
        long expected = 123L;
        when(_local.getEventCount(isSubject(), eq("queue-name"))).thenReturn(expected);

        long actual = databusClient(true).getEventCount("queue-name");

        assertEquals(actual, expected);
        verify(_local).getEventCount(isSubject(), eq("queue-name"));
        verifyNoMoreInteractions(_local);
    }

    @Test
    public void testGetEventCountUpToPartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.of("queue-name"))
                .getEventCountUpTo(createSubject(), "queue-name", 123L);
    }

    @Test
    public void testGetEventCountUpTo() {
        long expected = 123L;
        when(_client.getEventCountUpTo(isSubject(), eq("queue-name"), eq(10000L))).thenReturn(expected);

        long actual = databusClient().getEventCountUpTo("queue-name", 10000L);

        assertEquals(actual, expected);
        verify(_client).getEventCountUpTo(isSubject(), eq("queue-name"), eq(10000L));
        verifyNoMoreInteractions(_local, _client);
    }

    @Test
    public void testGetEventCountUpToPartitioned() {
        long expected = 123L;
        when(_local.getEventCountUpTo(isSubject(), eq("queue-name"), eq(10000L))).thenReturn(expected);

        long actual = databusClient(true).getEventCountUpTo("queue-name", 10000L);

        assertEquals(actual, expected);
        verify(_local).getEventCountUpTo(isSubject(), eq("queue-name"), eq(10000L));
        verifyNoMoreInteractions(_local);
    }

    @Test
    public void testGetClaimCountPartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.of("queue-name"))
                .getClaimCount(createSubject(), "queue-name");
    }

    @Test
    public void testGetClaimCount() {
        long expected = 123L;
        when(_client.getClaimCount(isSubject(), eq("queue-name"))).thenReturn(expected);

        long actual = databusClient(false).getClaimCount("queue-name");

        assertEquals(actual, expected);
        verify(_client).getClaimCount(isSubject(), eq("queue-name"));
        verifyNoMoreInteractions(_local, _client);
    }

    @Test
    public void testGetClaimCountPartitioned() {
        long expected = 123L;
        when(_local.getClaimCount(isSubject(), eq("queue-name"))).thenReturn(expected);

        long actual = databusClient(true).getClaimCount("queue-name");

        assertEquals(actual, expected);
        verify(_local).getClaimCount(isSubject(), eq("queue-name"));
        verifyNoMoreInteractions(_local);
    }

    @Test
    public void testPeekPartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.of("queue-name"))
                .peek(createSubject(), "queue-name", 123);
    }

    @Test
    public void testPeek() {
        testPeek(true);
    }

    @Test
    public void testPeekExcludeTags() {
        testPeek(false);
    }

    private void testPeek(boolean includeTags) {
        List<Event> peekResults = ImmutableList.of(
                new Event("id-1", ImmutableMap.of("key-1", "value-1"), ImmutableList.<List<String>>of(ImmutableList.<String>of("tag-1"))),
                new Event("id-2", ImmutableMap.of("key-2", "value-2"), ImmutableList.<List<String>>of(ImmutableList.<String>of("tag-2"))));
        when(_client.peek(isSubject(), eq("queue-name"), eq(123))).thenReturn(peekResults.iterator());

        List<Event> expected;
        List<Event> actual;

        if (includeTags) {
            // This is the default peek behavior
            expected = peekResults;
            actual = ImmutableList.copyOf(databusClient().peek("queue-name", 123));
        } else {
            // Tags won't be returned
            expected = ImmutableList.of(
                    new Event("id-1", ImmutableMap.of("key-1", "value-1"), ImmutableList.<List<String>>of()),
                    new Event("id-2", ImmutableMap.of("key-2", "value-2"), ImmutableList.<List<String>>of()));

            // Must make API call directly since only older databus clients don't automatically include tags
            // and the current databus client always does.
            actual = _resourceTestRule.client().resource("/bus/1/queue-name/peek")
                    .queryParam("limit", "123")
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, APIKEY_DATABUS)
                    .get(new GenericType<List<Event>>() {});
        }

        assertEquals(actual, expected);
        verify(_client).peek(isSubject(), eq("queue-name"), eq(123));
        verifyNoMoreInteractions(_local, _client);
    }

    @Test
    public void testPollPartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.of("queue-name"))
                .poll(createSubject(), "queue-name", Duration.ofSeconds(15), 123);
    }

    @Test
    public void testPoll() {
        testPoll(true);
    }

    @Test
    public void testPollExcludeTags() {
        testPoll(false);
    }

    private void testPoll(boolean includeTags) {
        List<Event> pollResults = ImmutableList.of(
                new Event("id-1", ImmutableMap.of("key-1", "value-1"), ImmutableList.<List<String>>of(ImmutableList.<String>of("tag-1"))),
                new Event("id-2", ImmutableMap.of("key-2", "value-2"), ImmutableList.<List<String>>of(ImmutableList.<String>of("tag-2"))));
        when(_client.poll(isSubject(), eq("queue-name"), eq(Duration.ofSeconds(15)), eq(123)))
                .thenReturn(new PollResult(pollResults.iterator(), 2, false));

        List<Event> expected;
        List<Event> actual;

        if (includeTags) {
            // This is the default poll behavior
            PollResult pollResult = databusClient().poll("queue-name", Duration.ofSeconds(15), 123);
            assertFalse(pollResult.hasMoreEvents());
            actual = ImmutableList.copyOf(pollResult.getEventIterator());
            expected = pollResults;
        } else {
            // Tags won't be returned
            expected = ImmutableList.of(
                    new Event("id-1", ImmutableMap.of("key-1", "value-1"), ImmutableList.<List<String>>of()),
                    new Event("id-2", ImmutableMap.of("key-2", "value-2"), ImmutableList.<List<String>>of()));

            // Must make API call directly since only older databus clients don't automatically include tags
            // and the current databus client always does.
            actual = _resourceTestRule.client().resource("/bus/1/queue-name/poll")
                    .queryParam("limit", "123")
                    .queryParam("ttl", "15")
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, APIKEY_DATABUS)
                    .get(new GenericType<List<Event>>() {});
        }

        assertEquals(actual, expected);
        verify(_client).poll(isSubject(), eq("queue-name"), eq(Duration.ofSeconds(15)), eq(123));
        verifyNoMoreInteractions(_local, _client);
    }

    @Test
    public void testLongPoll() throws Exception {
        testLongPoll(true);
    }

    @Test
    public void testLongPollExcludeTags() throws Exception {
        testLongPoll(false);
    }

    private void testLongPoll(boolean includeTags) throws Exception {
        // Resource tests don't support asynchronous requests, so do the next best thing and use a DatabusResourcePoller
        // directly.

        ScheduledExecutorService keepAliveService = Executors.newSingleThreadScheduledExecutor();
        ScheduledExecutorService pollService = Executors.newSingleThreadScheduledExecutor();

        try {
            DatabusResourcePoller poller = new DatabusResourcePoller(
                    Optional.of(new LongPollingExecutorServices(pollService, keepAliveService)), new MetricRegistry());

            SubjectDatabus databus = mock(SubjectDatabus.class);
            List<Event> pollResults = ImmutableList.of(
                    new Event("id-1", ImmutableMap.of("key-1", "value-1"), ImmutableList.<List<String>>of(ImmutableList.<String>of("tag-1"))),
                    new Event("id-2", ImmutableMap.of("key-2", "value-2"), ImmutableList.<List<String>>of(ImmutableList.<String>of("tag-2"))));
            //noinspection unchecked
            when(databus.poll(isSubject(), eq("queue-name"), eq(Duration.ofSeconds(10)), eq(100)))
                    .thenReturn(new PollResult(Collections.emptyIterator(), 0, false))
                    .thenReturn(new PollResult(pollResults.iterator(), 2, true));

            List<Event> expected;
            Class<? extends EventViews.ContentOnly> view;

            if (includeTags) {
                // This is the default poll behavior
                expected = pollResults;
                view = EventViews.WithTags.class;
            } else {
                // Tags won't be returned
                expected = ImmutableList.of(
                        new Event("id-1", ImmutableMap.of("key-1", "value-1"), ImmutableList.<List<String>>of()),
                        new Event("id-2", ImmutableMap.of("key-2", "value-2"), ImmutableList.<List<String>>of()));
                view = EventViews.ContentOnly.class;
            }

            final StringWriter out = new StringWriter();
            final AtomicBoolean complete = new AtomicBoolean(false);

            HttpServletRequest request = setupLongPollingTest(out, complete);

            PeekOrPollResponseHelper helper = new PeekOrPollResponseHelper(view);
            poller.poll(createSubject(), databus, "queue-name", Duration.ofSeconds(10), 100, request, false, helper);

            long failTime = System.currentTimeMillis() + Duration.ofSeconds(10).toMillis();

            while (!complete.get() && System.currentTimeMillis() < failTime) {
                Thread.sleep(100);
            }

            assertTrue(complete.get());

            List<Event> actual = JsonHelper.convert(
                    JsonHelper.fromJson(out.toString(), List.class), new TypeReference<List<Event>>() {
                    });

            assertEquals(actual, expected);
            verify(request).startAsync();
            verify(request.getAsyncContext()).complete();
        } finally {
            keepAliveService.shutdownNow();
            pollService.shutdownNow();
        }
    }

    @Test
    public void testLongPollSynchronousFailure()
            throws Exception {
        // Resource tests don't support asynchronous requests, so do the next best thing and use a DatabusResourcePoller
        // directly.

        ScheduledExecutorService keepAliveService = Executors.newSingleThreadScheduledExecutor();
        ScheduledExecutorService pollService = Executors.newSingleThreadScheduledExecutor();

        try {
            DatabusResourcePoller poller = new DatabusResourcePoller(
                    Optional.of(new LongPollingExecutorServices(pollService, keepAliveService)), new MetricRegistry());

            SubjectDatabus databus = mock(SubjectDatabus.class);
            when(databus.poll(isSubject(), eq("queue-name"), eq(Duration.ofSeconds(10)), eq(100)))
                    .thenThrow(new RuntimeException("Simulated read failure from Cassandra"));

            final StringWriter out = new StringWriter();
            final AtomicBoolean complete = new AtomicBoolean(false);

            HttpServletRequest request = setupLongPollingTest(out, complete);

            try {
                PeekOrPollResponseHelper helper = new PeekOrPollResponseHelper(EventViews.WithTags.class);
                poller.poll(createSubject(), databus, "queue-name", Duration.ofSeconds(10), 100, request, false, helper);
                fail("RuntimeException not thrown");
            } catch (RuntimeException e) {
                assertEquals(e.getMessage(), "Simulated read failure from Cassandra");
            }

            verify(request, never()).startAsync();
        } finally {
            keepAliveService.shutdownNow();
            pollService.shutdownNow();
        }
    }

    @Test
    public void testLongPollAsyncFailure()
            throws Exception {
        // Resource tests don't support asynchronous requests, so do the next best thing and use a DatabusResourcePoller
        // directly.

        ScheduledExecutorService keepAliveService = Executors.newSingleThreadScheduledExecutor();
        ScheduledExecutorService pollService = Executors.newSingleThreadScheduledExecutor();

        try {
            DatabusResourcePoller poller = new DatabusResourcePoller(
                    Optional.of(new LongPollingExecutorServices(pollService, keepAliveService)), new MetricRegistry());

            SubjectDatabus databus = mock(SubjectDatabus.class);
            when(databus.poll(isSubject(), eq("queue-name"), eq(Duration.ofSeconds(10)), eq(100)))
                    .thenReturn(new PollResult(Collections.emptyIterator(), 0, false))
                    .thenThrow(new RuntimeException("Simulated read failure from Cassandra"));

            final StringWriter out = new StringWriter();
            final AtomicBoolean complete = new AtomicBoolean(false);

            HttpServletRequest request = setupLongPollingTest(out, complete);

            PeekOrPollResponseHelper helper = new PeekOrPollResponseHelper(EventViews.WithTags.class);
            poller.poll(createSubject(), databus, "queue-name", Duration.ofSeconds(10), 100, request, false, helper);

            long failTime = System.currentTimeMillis() + Duration.ofSeconds(10).toMillis();

            while (!complete.get() && System.currentTimeMillis() < failTime) {
                Thread.sleep(100);
            }

            // Because the first poll completed successfully the expected response is a 200 with an empty list.

            assertTrue(complete.get());

            List<Event> actual = JsonHelper.convert(
                    JsonHelper.fromJson(out.toString(), List.class), new TypeReference<List<Event>>() {});

            assertEquals(actual, ImmutableList.<Event>of());
            verify(request).startAsync();
            verify(request.getAsyncContext()).complete();
        } finally {
            keepAliveService.shutdownNow();
            pollService.shutdownNow();
        }
    }

    private HttpServletRequest setupLongPollingTest(final StringWriter out, final AtomicBoolean complete)
            throws Exception {

        ServletOutputStream servletOutputStream = new ServletOutputStream() {
            @Override
            public void write(int b) throws IOException {
                out.write(b);
            }
        };

        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getOutputStream()).thenReturn(servletOutputStream);

        AsyncContext asyncContext = mock(AsyncContext.class);
        when(asyncContext.getResponse()).thenReturn(response);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                complete.set(true);
                return null;
            }
        }).when(asyncContext).complete();

        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.startAsync()).thenReturn(asyncContext);
        when(request.getAsyncContext()).thenReturn(asyncContext);

        return request;
    }

    @Test
    public void testPollUnauthorized() {
        try {
            unauthorizedDatabusClient(false).poll("queue-name", Duration.ofSeconds(15), 123);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof UnauthorizedException);
        }

        verifyNoMoreInteractions(_client);
    }

    @Test
    public void testPollNotOwner() {
        Duration ttl = Duration.ofSeconds(15);
        int limit = 123;

        when(_client.poll(isSubject(), eq("queue-name"), eq(ttl), eq(limit)))
                .thenThrow(new UnauthorizedSubscriptionException("Not owner", "queue-name"));

        try {
            databusClient().poll("queue-name", ttl, limit);
            fail();
        } catch (UnauthorizedException e) {
            // expected
        }

        verify(_client).poll(isSubject(), eq("queue-name"), eq(ttl), eq(limit));
        verifyNoMoreInteractions(_client);
    }

    @Test
    public void testPollPartitioned() {
        List<Event> expected = ImmutableList.of(
                new Event("id-1", ImmutableMap.of("key-1", "value-1"), ImmutableList.<List<String>>of()),
                new Event("id-2", ImmutableMap.of("key-2", "value-2"), ImmutableList.<List<String>>of()));
        when(_local.poll(isSubject(), eq("queue-name"), eq(Duration.ofSeconds(15)), eq(123)))
                .thenReturn(new PollResult(expected.iterator(), 2, true));

        PollResult actual = databusClient(true).poll("queue-name", Duration.ofSeconds(15), 123);

        assertEquals(ImmutableList.copyOf(actual.getEventIterator()), expected);
        assertTrue(actual.hasMoreEvents());
        verify(_local).poll(isSubject(), eq("queue-name"), eq(Duration.ofSeconds(15)), eq(123));
        verifyNoMoreInteractions(_local);
    }

    @Test
    public void testPollUnauthorizedPartitioned() {
        try {
            unauthorizedDatabusClient(true).poll("queue-name", Duration.ofSeconds(15), 123);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof UnauthorizedException);
        }

        verifyNoMoreInteractions(_client);
    }

    @Test
    public void testPollPartitionTimeout() {
        when(_client.poll(isSubject(), eq("queue-name"), eq(Duration.ofSeconds(15)), eq(123)))
                .thenThrow(new PartitionForwardingException(new ConnectTimeoutException()));

        try {
            databusClient().poll("queue-name", Duration.ofSeconds(15), 123);
        } catch (ServiceUnavailableException e) {
            // Ok
        }

        verify(_client).poll(isSubject(), eq("queue-name"), eq(Duration.ofSeconds(15)), eq(123));
        verifyNoMoreInteractions(_client);
    }

    @Test
    public void testRenewPartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.of("queue-name"))
                .renew(createSubject(), "queue-name", ImmutableList.of("id-1", "id-2"), Duration.ofSeconds(15));
    }

    @Test
    public void testRenew() {
        databusClient().renew("queue-name", ImmutableList.of("id-1", "id-2"), Duration.ofSeconds(15));

        verify(_client).renew(isSubject(), eq("queue-name"), eq(ImmutableList.of("id-1", "id-2")), eq(Duration.ofSeconds(15)));
        verifyNoMoreInteractions(_local, _client);
    }

    @Test
    public void testRenewPartitioned() {
        databusClient(true).renew("queue-name", ImmutableList.of("id-1", "id-2"), Duration.ofSeconds(15));

        verify(_local).renew(isSubject(), eq("queue-name"), eq(ImmutableList.of("id-1", "id-2")), eq(Duration.ofSeconds(15)));
        verifyNoMoreInteractions(_local);
    }

    @Test
    public void testAcknowledgePartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.of("queue-name"))
                .acknowledge(createSubject(), "queue-name", ImmutableList.of("id-1", "id-2"));
    }

    @Test
    public void testAcknowledge() {
        databusClient().acknowledge("queue-name", ImmutableList.of("id-1", "id-2"));

        verify(_client).acknowledge(isSubject(), eq("queue-name"), eq(ImmutableList.of("id-1", "id-2")));
        verifyNoMoreInteractions(_local, _client);
    }

    @Test
    public void testAcknowledgePartitioned() {
        databusClient(true).acknowledge("queue-name", ImmutableList.of("id-1", "id-2"));

        verify(_local).acknowledge(isSubject(), eq("queue-name"), eq(ImmutableList.of("id-1", "id-2")));
        verifyNoMoreInteractions(_local);
    }

    @Test
    public void testReplay() {
        when(_local.replayAsyncSince(isSubject(), eq("queue-name"), isNull())).thenReturn("replayId1");
        String replayId = databusClient().replayAsync("queue-name");

        verify(_local).replayAsyncSince(isSubject(), eq("queue-name"), isNull());
        verifyNoMoreInteractions(_local);
        assertEquals("replayId1", replayId);
    }

    @Test
    public void testReplaySince() {
        Date now = Date.from(Instant.now());
        when(_local.replayAsyncSince(isSubject(), eq("queue-name"), eq(now))).thenReturn("replayId1");
        String replayId = databusClient().replayAsyncSince("queue-name", now);

        verify(_local).replayAsyncSince(isSubject(), eq("queue-name"), eq(now));
        verifyNoMoreInteractions(_local);
        assertEquals("replayId1", replayId);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReplaySinceWithSinceTooFarBackInTime() {
        Date since = Date.from(Instant.now().minus(DatabusChannelConfiguration.REPLAY_TTL));
        when(_local.replayAsyncSince(isSubject(), eq("queue-name"), eq(since))).thenReturn("replayId1");
        databusClient().replayAsyncSince("queue-name", since);
    }

    @Test
    public void testReplayStatus() {
        ReplaySubscriptionStatus expected = new ReplaySubscriptionStatus("queue-name", ReplaySubscriptionStatus.Status.IN_PROGRESS);
        when(_local.getReplayStatus(isSubject(), eq("replayId1"))).thenReturn(expected);

        ReplaySubscriptionStatus status = databusClient().getReplayStatus("replayId1");

        verify(_local).getReplayStatus(isSubject(), eq("replayId1"));
        verifyNoMoreInteractions(_local);
        assertEquals("queue-name", status.getSubscription());
        assertEquals(ReplaySubscriptionStatus.Status.IN_PROGRESS, status.getStatus());
    }

    @Test
    public void testMove() {
        when(_local.moveAsync(isSubject(), eq("queue-src"), eq("queue-dest"))).thenReturn("moveId1");

        String moveId = databusClient().moveAsync("queue-src", "queue-dest");

        verify(_local).moveAsync(isSubject(), eq("queue-src"), eq("queue-dest"));
        verifyNoMoreInteractions(_local);
        assertEquals("moveId1", moveId);
    }

    @Test
    public void testMoveStatus() {
        MoveSubscriptionStatus expected = new MoveSubscriptionStatus("queue-src", "queue-dest", MoveSubscriptionStatus.Status.IN_PROGRESS);
        when(_local.getMoveStatus(isSubject(), eq("moveId1"))).thenReturn(expected);

        MoveSubscriptionStatus status = databusClient().getMoveStatus("moveId1");

        verify(_local).getMoveStatus(isSubject(), eq("moveId1"));
        verifyNoMoreInteractions(_local);
        assertEquals("queue-src", status.getFrom());
        assertEquals("queue-dest", status.getTo());
        assertEquals(MoveSubscriptionStatus.Status.IN_PROGRESS, status.getStatus());
    }

    @Test
    public void testUnclaimAllPartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.of("queue-name"))
                .unclaimAll(createSubject(), "queue-name");
    }

    @Test
    public void testUnclaimAll() {
        databusClient().unclaimAll("queue-name");

        verify(_client).unclaimAll(isSubject(), eq("queue-name"));
        verifyNoMoreInteractions(_local, _client);
    }

    @Test
    public void testUnclaimAllPartitioned() {
        databusClient(true).unclaimAll("queue-name");

        verify(_local).unclaimAll(isSubject(), eq("queue-name"));
        verifyNoMoreInteractions(_local);
    }

    @Test
    public void testPurgePartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.of("queue-name"))
                .purge(createSubject(), "queue-name");
    }

    @Test
    public void testPurge() {
        databusClient().purge("queue-name");

        verify(_client).purge(isSubject(), eq("queue-name"));
        verifyNoMoreInteractions(_local, _client);
    }

    @Test
    public void testPurgePartitioned() {
        databusClient(true).purge("queue-name");

        verify(_local).purge(isSubject(), eq("queue-name"));
        verifyNoMoreInteractions(_local);
    }

    private void assertSubscriptionEquals(Subscription expected, Subscription actual) {
        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getTableFilter(), expected.getTableFilter());
        assertEquals(actual.getExpiresAt(), expected.getExpiresAt());
        assertEquals(actual.getEventTtl(), expected.getEventTtl());
    }

    public static class ContextInjectableProvider<T> extends SingletonTypeInjectableProvider<Context, T> {
        public ContextInjectableProvider(Type type, T instance) {
            super(type, instance);
        }
    }
}
