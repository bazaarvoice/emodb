package test.integration.databus;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.common.api.UnauthorizedException;
import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.databus.api.AuthDatabus;
import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.databus.api.DefaultSubscription;
import com.bazaarvoice.emodb.databus.api.Event;
import com.bazaarvoice.emodb.databus.api.EventViews;
import com.bazaarvoice.emodb.databus.api.MoveSubscriptionStatus;
import com.bazaarvoice.emodb.databus.api.ReplaySubscriptionStatus;
import com.bazaarvoice.emodb.databus.api.Subscription;
import com.bazaarvoice.emodb.databus.api.UnauthorizedSubscriptionException;
import com.bazaarvoice.emodb.databus.api.UnknownSubscriptionException;
import com.bazaarvoice.emodb.databus.client.DatabusAuthenticator;
import com.bazaarvoice.emodb.databus.client.DatabusClient;
import com.bazaarvoice.emodb.databus.core.DatabusChannelConfiguration;
import com.bazaarvoice.emodb.databus.core.DatabusEventStore;
import com.bazaarvoice.emodb.databus.core.DatabusFactory;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.core.UpdateRef;
import com.bazaarvoice.emodb.test.ResourceTest;
import com.bazaarvoice.emodb.web.resources.databus.DatabusClientSubjectProxy;
import com.bazaarvoice.emodb.web.resources.databus.DatabusResource1;
import com.bazaarvoice.emodb.web.resources.databus.DatabusResourcePoller;
import com.bazaarvoice.emodb.web.resources.databus.LongPollingExecutorServices;
import com.bazaarvoice.emodb.web.resources.databus.PeekOrPollResponseHelper;
import com.bazaarvoice.ostrich.PartitionContextBuilder;
import com.bazaarvoice.ostrich.pool.OstrichAccessors;
import com.bazaarvoice.ostrich.pool.PartitionContextValidator;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.spi.inject.SingletonTypeInjectableProvider;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
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
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests the api calls made via the Jersey HTTP client {@link DatabusClient} are
 * interpreted correctly by the Jersey HTTP resource {@link DatabusResource1}.
 */
public class DatabusJerseyTest extends ResourceTest {
    private static final String APIKEY_DATABUS = "databus-key";
    private static final String INTERNAL_ID_DATABUS = "databus-id";
    private static final String APIKEY_UNAUTHORIZED = "unauthorized-key";
    private static final String INTERNAL_ID_UNAUTHORIZED = "unauthorized-id";

    private final PartitionContextValidator<AuthDatabus> _pcxtv =
            OstrichAccessors.newPartitionContextTest(AuthDatabus.class, DatabusClient.class);
    private final DatabusFactory _factory = mock(DatabusFactory.class);
    private final Databus _server = mock(Databus.class);
    private final DatabusClientSubjectProxy _proxyProvider = mock(DatabusClientSubjectProxy.class);
    private final Databus _proxy = mock(Databus.class);

    @Rule
    public ResourceTestRule _resourceTestRule = setupResourceTestRule(
            Collections.<Object>singletonList(new DatabusResource1(_factory, _proxyProvider, mock(DatabusEventStore.class), new DatabusResourcePoller(new MetricRegistry()))),
                    new ApiKey(APIKEY_DATABUS, INTERNAL_ID_DATABUS, ImmutableSet.of("databus-role")),
                    new ApiKey(APIKEY_UNAUTHORIZED, INTERNAL_ID_UNAUTHORIZED, ImmutableSet.of("unauthorized-role")),
                    "databus");

    @After
    public void tearDownMocksAndClearState() {
        verifyNoMoreInteractions(_server, _proxy);
        reset(_factory, _server, _proxyProvider, _proxy);
    }

    private Databus databusClient() {
        when(_factory.forOwner(INTERNAL_ID_DATABUS)).thenReturn(_server);
        when(_proxyProvider.forSubject(argThat(matchesKey(APIKEY_DATABUS)))).thenReturn(_proxy);
        return DatabusAuthenticator.proxied(new DatabusClient(URI.create("/bus/1"), new JerseyEmoClient(_resourceTestRule.client())))
                .usingCredentials(APIKEY_DATABUS);
    }

    private Databus databusClient(boolean partitioned) {
        when(_factory.forOwner(INTERNAL_ID_DATABUS)).thenReturn(_server);
        when(_proxyProvider.forSubject(argThat(matchesKey(APIKEY_DATABUS)))).thenReturn(_proxy);
        return DatabusAuthenticator.proxied(new DatabusClient(URI.create("/bus/1"), partitioned, new JerseyEmoClient(_resourceTestRule.client())))
                .usingCredentials(APIKEY_DATABUS);
    }

    private Databus unauthorizedDatabusClient(boolean partitioned) {
        when(_factory.forOwner(INTERNAL_ID_UNAUTHORIZED)).thenReturn(_server);
        when(_proxyProvider.forSubject(argThat(matchesKey(APIKEY_UNAUTHORIZED)))).thenReturn(_proxy);
        return DatabusAuthenticator.proxied(new DatabusClient(URI.create("/bus/1"), partitioned, new JerseyEmoClient(_resourceTestRule.client())))
                .usingCredentials(APIKEY_UNAUTHORIZED);
    }

    private Matcher<Subject> matchesKey(final String apiKey) {
        return new BaseMatcher<Subject>() {
            @Override
            public boolean matches(Object o) {
                Subject subject = (Subject) o;
                return subject != null && subject.getId().equals(apiKey);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("API key ").appendText(apiKey);
            }
        };
    }

    @Test
    public void testListSubscriptions() {
        _pcxtv.expect(PartitionContextBuilder.empty())
                .listSubscriptions(APIKEY_DATABUS, "from-queue", 123L);
    }

    @Test
    public void testListSubscriptions1() {
        when(_server.listSubscriptions(null, Long.MAX_VALUE)).thenReturn(Iterators.<Subscription>emptyIterator());

        Iterator<Subscription> actual = databusClient().listSubscriptions(null, Long.MAX_VALUE);

        assertFalse(actual.hasNext());
        verify(_server).listSubscriptions(null, Long.MAX_VALUE);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testListSubscriptions2() {
        Date now = new Date();
        List<Subscription> expected = ImmutableList.<Subscription>of(
                new DefaultSubscription("queue-name1", Conditions.alwaysTrue(), now, Duration.standardHours(48)),
                new DefaultSubscription("queue-name2", Conditions.intrinsic(Intrinsic.TABLE, "test"), now, Duration.standardDays(7)));
        when(_server.listSubscriptions("queue-name", 123L)).thenReturn(expected.iterator());

        List<Subscription> actual = ImmutableList.copyOf(
                databusClient().listSubscriptions("queue-name", 123L));

        assertEquals(actual.size(), expected.size());
        for (int i = 0; i < actual.size(); i++) {
            assertSubscriptionEquals(actual.get(i), expected.get(i));
        }
        verify(_server).listSubscriptions("queue-name", 123L);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testSubscribePartitionContext() {
        Condition condition = Conditions.intrinsic(Intrinsic.TABLE, "test");
        Duration subscriptionTtl = Duration.standardDays(15);
        Duration eventTtl = Duration.standardDays(2);

        // By default we ignore any events tagged with "re-etl"
        Condition expectedCondition = Conditions.and(condition,
                Conditions.not(Conditions.mapBuilder().matches(UpdateRef.TAGS_NAME, Conditions.containsAny("re-etl")).build()));

        _pcxtv.expect(PartitionContextBuilder.empty())
                .subscribe(APIKEY_DATABUS, "queue-name", expectedCondition, subscriptionTtl, eventTtl);
    }

    @Test
    public void testSubscribePartitionContextWithoutIgnoringAnyEvents() {
        Condition condition = Conditions.intrinsic(Intrinsic.TABLE, "test");
        Duration subscriptionTtl = Duration.standardDays(15);
        Duration eventTtl = Duration.standardDays(2);

        _pcxtv.expect(PartitionContextBuilder.empty())
                .subscribe(APIKEY_DATABUS, "queue-name", condition, subscriptionTtl, eventTtl, false);
    }

    @Test
    public void testSubscribe() {
        Condition condition = Conditions.intrinsic(Intrinsic.TABLE, "test");
        Duration subscriptionTtl = Duration.standardDays(15);
        Duration eventTtl = Duration.standardDays(2);

        databusClient().subscribe("queue-name", condition, subscriptionTtl, eventTtl);

        verify(_server).subscribe("queue-name", condition, subscriptionTtl, eventTtl, true);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testSubscribeWithoutIgnoringAnyEvents() {
        Condition condition = Conditions.intrinsic(Intrinsic.TABLE, "test");
        Duration subscriptionTtl = Duration.standardDays(15);
        Duration eventTtl = Duration.standardDays(2);

        databusClient().subscribe("queue-name", condition, subscriptionTtl, eventTtl, false);

        verify(_server).subscribe("queue-name", condition, subscriptionTtl, eventTtl, false);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testSubscribeNotOwner() {
        Condition condition = Conditions.intrinsic(Intrinsic.TABLE, "test");
        Duration subscriptionTtl = Duration.standardDays(15);
        Duration eventTtl = Duration.standardDays(2);

        doThrow(new UnauthorizedSubscriptionException("Not owner", "queue-name")).
            when(_server).subscribe("queue-name", condition, subscriptionTtl, eventTtl, true);

        try {
            databusClient().subscribe("queue-name", condition, subscriptionTtl, eventTtl);
            fail();
        } catch (UnauthorizedSubscriptionException e) {
            assertEquals(e.getSubscription(), "queue-name");
        }

        verify(_server).subscribe("queue-name", condition, subscriptionTtl, eventTtl, true);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testUnsubscribePartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.of("queue-name"))
                .unsubscribe(APIKEY_DATABUS, "queue-name");
    }

    @Test
    public void testUnsubscribe() {
        databusClient().unsubscribe("queue-name");

        verify(_proxyProvider).forSubject(argThat(matchesKey(APIKEY_DATABUS)));
        verify(_proxy).unsubscribe("queue-name");
        verifyNoMoreInteractions(_proxyProvider, _proxy);
    }

    @Test
    public void testUnsubscribePartitioned() {
        databusClient(true).unsubscribe("queue-name");

        verify(_server).unsubscribe("queue-name");
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetSubscriptionPartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.empty())
                .getSubscription(APIKEY_DATABUS, "queue-name");
    }

    @Test
    public void testGetSubscription() {
        Date now = new Date();
        Subscription expected = new DefaultSubscription("queue-name", Conditions.alwaysTrue(), now, Duration.standardHours(48));
        when(_server.getSubscription("queue-name")).thenReturn(expected);

        Subscription actual = databusClient().getSubscription("queue-name");

        assertSubscriptionEquals(expected, actual);
        verify(_server).getSubscription("queue-name");
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetUnknownSubscription() {
        UnknownSubscriptionException expected = new UnknownSubscriptionException("queue-name");
        when(_server.getSubscription("queue-name")).thenThrow(expected);

        try {
            databusClient().getSubscription("queue-name");
            fail();
        } catch (UnknownSubscriptionException actual) {
            assertEquals(actual.getSubscription(), "queue-name");
            assertNotSame(actual, expected);  // Should be recreated by exception mappers
        }

        verify(_server).getSubscription("queue-name");
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetEventCountPartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.of("queue-name"))
                .getEventCount(APIKEY_DATABUS, "queue-name");
    }

    @Test
    public void testGetEventCount() {
        long expected = 123L;
        when(_proxy.getEventCount("queue-name")).thenReturn(expected);

        long actual = databusClient().getEventCount("queue-name");

        assertEquals(actual, expected);
        verify(_proxyProvider).forSubject(argThat(matchesKey(APIKEY_DATABUS)));
        verify(_proxy).getEventCount("queue-name");
        verifyNoMoreInteractions(_proxyProvider, _proxy);
    }

    @Test
    public void testGetEventCountPartitioned() {
        long expected = 123L;
        when(_server.getEventCount("queue-name")).thenReturn(expected);

        long actual = databusClient(true).getEventCount("queue-name");

        assertEquals(actual, expected);
        verify(_server).getEventCount("queue-name");
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetEventCountUpToPartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.of("queue-name"))
                .getEventCountUpTo(APIKEY_DATABUS, "queue-name", 123L);
    }

    @Test
    public void testGetEventCountUpTo() {
        long expected = 123L;
        when(_proxy.getEventCountUpTo("queue-name", 10000L)).thenReturn(expected);

        long actual = databusClient().getEventCountUpTo("queue-name", 10000L);

        assertEquals(actual, expected);
        verify(_proxyProvider).forSubject(argThat(matchesKey(APIKEY_DATABUS)));
        verify(_proxy).getEventCountUpTo("queue-name", 10000L);
        verifyNoMoreInteractions(_proxyProvider, _proxy);
    }

    @Test
    public void testGetEventCountUpToPartitioned() {
        long expected = 123L;
        when(_server.getEventCountUpTo("queue-name", 10000L)).thenReturn(expected);

        long actual = databusClient(true).getEventCountUpTo("queue-name", 10000L);

        assertEquals(actual, expected);
        verify(_server).getEventCountUpTo("queue-name", 10000L);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetClaimCountPartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.of("queue-name"))
                .getClaimCount(APIKEY_DATABUS, "queue-name");
    }

    @Test
    public void testGetClaimCount() {
        long expected = 123L;
        when(_proxy.getClaimCount("queue-name")).thenReturn(expected);

        long actual = databusClient(false).getClaimCount("queue-name");

        assertEquals(actual, expected);
        verify(_proxyProvider).forSubject(argThat(matchesKey(APIKEY_DATABUS)));
        verify(_proxy).getClaimCount("queue-name");
        verifyNoMoreInteractions(_proxyProvider, _proxy);
    }

    @Test
    public void testGetClaimCountPartitioned() {
        long expected = 123L;
        when(_server.getClaimCount("queue-name")).thenReturn(expected);

        long actual = databusClient(true).getClaimCount("queue-name");

        assertEquals(actual, expected);
        verify(_server).getClaimCount("queue-name");
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testPeekPartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.of("queue-name"))
                .peek(APIKEY_DATABUS, "queue-name", 123);
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
        when(_proxy.peek("queue-name", 123)).thenReturn(peekResults);
        when(_proxyProvider.forSubject(argThat(matchesKey(APIKEY_DATABUS)))).thenReturn(_proxy);

        List<Event> expected;
        List<Event> actual;

        if (includeTags) {
            // This is the default peek behavior
            expected = peekResults;
            actual = databusClient().peek("queue-name", 123);
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
        verify(_proxyProvider).forSubject(argThat(matchesKey(APIKEY_DATABUS)));
        verify(_proxy).peek("queue-name", 123);
        verifyNoMoreInteractions(_proxyProvider, _proxy);
    }

    @Test
    public void testPollPartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.of("queue-name"))
                .poll(APIKEY_DATABUS, "queue-name", Duration.standardSeconds(15), 123);
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
        when(_proxy.poll("queue-name", Duration.standardSeconds(15), 123)).thenReturn(pollResults);
        when(_proxyProvider.forSubject(argThat(matchesKey(APIKEY_DATABUS)))).thenReturn(_proxy);

        List<Event> expected;
        List<Event> actual;

        if (includeTags) {
            // This is the default poll behavior
            expected = databusClient().poll("queue-name", Duration.standardSeconds(15), 123);
            actual = pollResults;
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
        verify(_proxyProvider).forSubject(argThat(matchesKey(APIKEY_DATABUS)));
        verify(_proxy).poll("queue-name", Duration.standardSeconds(15), 123);
        verifyNoMoreInteractions(_proxyProvider, _proxy);
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

            Databus databus = mock(Databus.class);
            List<Event> emptyList = ImmutableList.of();
            List<Event> pollResults = ImmutableList.of(
                    new Event("id-1", ImmutableMap.of("key-1", "value-1"), ImmutableList.<List<String>>of(ImmutableList.<String>of("tag-1"))),
                    new Event("id-2", ImmutableMap.of("key-2", "value-2"), ImmutableList.<List<String>>of(ImmutableList.<String>of("tag-2"))));
            //noinspection unchecked
            when(databus.poll("queue-name", Duration.standardSeconds(10), 100))
                    .thenReturn(emptyList, pollResults);

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
            poller.poll(databus, "queue-name", Duration.standardSeconds(10), 100, request, false, helper);

            long failTime = System.currentTimeMillis() + Duration.standardSeconds(10).getMillis();

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

            Databus databus = mock(Databus.class);
            when(databus.poll("queue-name", Duration.standardSeconds(10), 100))
                    .thenThrow(new RuntimeException("Simulated read failure from Cassandra"));

            final StringWriter out = new StringWriter();
            final AtomicBoolean complete = new AtomicBoolean(false);

            HttpServletRequest request = setupLongPollingTest(out, complete);

            try {
                PeekOrPollResponseHelper helper = new PeekOrPollResponseHelper(EventViews.WithTags.class);
                poller.poll(databus, "queue-name", Duration.standardSeconds(10), 100, request, false, helper);
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

            Databus databus = mock(Databus.class);
            when(databus.poll("queue-name", Duration.standardSeconds(10), 100))
                    .thenReturn(ImmutableList.<Event>of())
                    .thenThrow(new RuntimeException("Simulated read failure from Cassandra"));

            final StringWriter out = new StringWriter();
            final AtomicBoolean complete = new AtomicBoolean(false);

            HttpServletRequest request = setupLongPollingTest(out, complete);

            PeekOrPollResponseHelper helper = new PeekOrPollResponseHelper(EventViews.WithTags.class);
            poller.poll(databus, "queue-name", Duration.standardSeconds(10), 100, request, false, helper);

            long failTime = System.currentTimeMillis() + Duration.standardSeconds(10).getMillis();

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
            unauthorizedDatabusClient(false).poll("queue-name", Duration.standardSeconds(15), 123);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof UnauthorizedException);
        }

        verifyNoMoreInteractions(_proxy);
    }

    @Test
    public void testPollNotOwner() {
        Duration ttl = Duration.standardSeconds(15);
        int limit = 123;

        when(_proxy.poll("queue-name", ttl, limit))
                .thenThrow(new UnauthorizedSubscriptionException("Not owner", "queue-name"));

        try {
            databusClient().poll("queue-name", ttl, limit);
            fail();
        } catch (UnauthorizedException e) {
            // expected
        }

        verify(_proxy).poll("queue-name", ttl, limit);
        verifyNoMoreInteractions(_proxy);
    }

    @Test
    public void testPollPartitioned() {
        List<Event> expected = ImmutableList.of(
                new Event("id-1", ImmutableMap.of("key-1", "value-1"), ImmutableList.<List<String>>of()),
                new Event("id-2", ImmutableMap.of("key-2", "value-2"), ImmutableList.<List<String>>of()));
        when(_server.poll("queue-name", Duration.standardSeconds(15), 123)).thenReturn(expected);

        List<Event> actual = databusClient(true).poll("queue-name", Duration.standardSeconds(15), 123);

        assertEquals(actual, expected);
        verify(_server).poll("queue-name", Duration.standardSeconds(15), 123);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testPollUnauthorizedPartitioned() {
        try {
            unauthorizedDatabusClient(true).poll("queue-name", Duration.standardSeconds(15), 123);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof UnauthorizedException);
        }

        verifyNoMoreInteractions(_proxy);
    }

    @Test
    public void testRenewPartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.of("queue-name"))
                .renew(APIKEY_DATABUS, "queue-name", ImmutableList.of("id-1", "id-2"), Duration.standardSeconds(15));
    }

    @Test
    public void testRenew() {
        databusClient().renew("queue-name", ImmutableList.of("id-1", "id-2"), Duration.standardSeconds(15));

        verify(_proxyProvider).forSubject(argThat(matchesKey(APIKEY_DATABUS)));
        verify(_proxy).renew("queue-name", ImmutableList.of("id-1", "id-2"), Duration.standardSeconds(15));
        verifyNoMoreInteractions(_proxyProvider, _proxy);
    }

    @Test
    public void testRenewPartitioned() {
        databusClient(true).renew("queue-name", ImmutableList.of("id-1", "id-2"), Duration.standardSeconds(15));

        verify(_server).renew("queue-name", ImmutableList.of("id-1", "id-2"), Duration.standardSeconds(15));
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testAcknowledgePartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.of("queue-name"))
                .acknowledge(APIKEY_DATABUS, "queue-name", ImmutableList.of("id-1", "id-2"));
    }

    @Test
    public void testAcknowledge() {
        databusClient().acknowledge("queue-name", ImmutableList.of("id-1", "id-2"));

        verify(_proxyProvider).forSubject(argThat(matchesKey(APIKEY_DATABUS)));
        verify(_proxy).acknowledge("queue-name", ImmutableList.of("id-1", "id-2"));
        verifyNoMoreInteractions(_proxyProvider, _proxy);
    }

    @Test
    public void testAcknowledgePartitioned() {
        databusClient(true).acknowledge("queue-name", ImmutableList.of("id-1", "id-2"));

        verify(_server).acknowledge("queue-name", ImmutableList.of("id-1", "id-2"));
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testReplay() {
        when(_server.replayAsyncSince("queue-name", null)).thenReturn("replayId1");
        String replayId = databusClient().replayAsync("queue-name");

        verify(_server).replayAsyncSince("queue-name", null);
        verifyNoMoreInteractions(_server);
        assertEquals(replayId, "replayId1");
    }

    @Test
    public void testReplaySince() {
        Date now = DateTime.now().toDate();
        when(_server.replayAsyncSince("queue-name", now)).thenReturn("replayId1");
        String replayId = databusClient().replayAsyncSince("queue-name", now);

        verify(_server).replayAsyncSince("queue-name", now);
        verifyNoMoreInteractions(_server);
        assertEquals(replayId, "replayId1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReplaySinceWithSinceTooFarBackInTime() {
        Date since = DateTime.now()
                .minus(DatabusChannelConfiguration.REPLAY_TTL).toDate();
        when(_server.replayAsyncSince("queue-name", since)).thenReturn("replayId1");
        databusClient().replayAsyncSince("queue-name", since);
    }

    @Test
    public void testReplayStatus() {
        ReplaySubscriptionStatus expected = new ReplaySubscriptionStatus("queue-name", ReplaySubscriptionStatus.Status.IN_PROGRESS);
        when(_server.getReplayStatus("replayId1")).thenReturn(expected);

        ReplaySubscriptionStatus status = databusClient().getReplayStatus("replayId1");

        verify(_server).getReplayStatus("replayId1");
        verifyNoMoreInteractions(_server);
        assertEquals(status.getSubscription(), "queue-name");
        assertEquals(status.getStatus(), ReplaySubscriptionStatus.Status.IN_PROGRESS);
    }

    @Test
    public void testMove() {
        when(_server.moveAsync("queue-src", "queue-dest")).thenReturn("moveId1");

        String moveId = databusClient().moveAsync("queue-src", "queue-dest");

        verify(_server).moveAsync("queue-src", "queue-dest");
        verifyNoMoreInteractions(_server);
        assertEquals(moveId, "moveId1");
    }

    @Test
    public void testMoveStatus() {
        MoveSubscriptionStatus expected = new MoveSubscriptionStatus("queue-src", "queue-dest", MoveSubscriptionStatus.Status.IN_PROGRESS);
        when(_server.getMoveStatus("moveId1")).thenReturn(expected);

        MoveSubscriptionStatus status = databusClient().getMoveStatus("moveId1");

        verify(_server).getMoveStatus("moveId1");
        verifyNoMoreInteractions(_server);
        assertEquals(status.getFrom(), "queue-src");
        assertEquals(status.getTo(), "queue-dest");
        assertEquals(status.getStatus(), MoveSubscriptionStatus.Status.IN_PROGRESS);
    }

    @Test
    public void testUnclaimAllPartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.of("queue-name"))
                .unclaimAll(APIKEY_DATABUS, "queue-name");
    }

    @Test
    public void testUnclaimAll() {
        databusClient().unclaimAll("queue-name");

        verify(_proxyProvider).forSubject(argThat(matchesKey(APIKEY_DATABUS)));
        verify(_proxy).unclaimAll("queue-name");
        verifyNoMoreInteractions(_proxyProvider, _proxy);
    }

    @Test
    public void testUnclaimAllPartitioned() {
        databusClient(true).unclaimAll("queue-name");

        verify(_server).unclaimAll("queue-name");
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testPurgePartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.of("queue-name"))
                .purge(APIKEY_DATABUS, "queue-name");
    }

    @Test
    public void testPurge() {
        databusClient().purge("queue-name");

        verify(_proxyProvider).forSubject(argThat(matchesKey(APIKEY_DATABUS)));
        verify(_proxy).purge("queue-name");
        verifyNoMoreInteractions(_proxyProvider, _proxy);
    }

    @Test
    public void testPurgePartitioned() {
        databusClient(true).purge("queue-name");

        verify(_server).purge("queue-name");
        verifyNoMoreInteractions(_server);
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
