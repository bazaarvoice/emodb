package test.integration.databus;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.databus.api.Event;
import com.bazaarvoice.emodb.databus.api.PollResult;
import com.bazaarvoice.emodb.databus.client.DatabusAuthenticator;
import com.bazaarvoice.emodb.databus.client.DatabusClient;
import com.bazaarvoice.emodb.databus.core.DatabusEventStore;
import com.bazaarvoice.emodb.test.ResourceTest;
import com.bazaarvoice.emodb.web.resources.databus.AbstractSubjectDatabus;
import com.bazaarvoice.emodb.web.resources.databus.DatabusResource1;
import com.bazaarvoice.emodb.web.resources.databus.DatabusResourcePoller;
import com.bazaarvoice.emodb.web.resources.databus.SubjectDatabus;
import com.bazaarvoice.ostrich.PartitionContextBuilder;
import com.bazaarvoice.ostrich.pool.OstrichAccessors;
import com.bazaarvoice.ostrich.pool.PartitionContextValidator;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.net.URI;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests the api calls made via the Jersey HTTP client {@link DatabusClient} are
 * interpreted correctly by the Jersey HTTP resource {@link DatabusResource1}.
 */
@SuppressWarnings("Duplicates")
public class HiddenFieldsDatabusJerseyTest extends ResourceTest {
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
        Collections.singletonList(new DatabusResource1(_local, _client, mock(DatabusEventStore.class), new DatabusResourcePoller(new MetricRegistry()))),
        new ApiKey(APIKEY_DATABUS, INTERNAL_ID_DATABUS, ImmutableSet.of("databus-role")),
        new ApiKey(APIKEY_UNAUTHORIZED, INTERNAL_ID_UNAUTHORIZED, ImmutableSet.of("unauthorized-role")),
        "databus");

    @After
    public void tearDownMocksAndClearState() {
        verifyNoMoreInteractions(_local, _client);
        reset(_local, _client);
    }

    private Databus databusClient() {
        return DatabusAuthenticator.proxied(new DatabusClient(URI.create("/bus/1"), new JerseyEmoClient(_resourceTestRule.client())))
            .usingCredentials(APIKEY_DATABUS);
    }

    private Matcher<Subject> matchesSubject(final String apiKey, final String internalId) {
        return new BaseMatcher<Subject>() {
            @Override
            public boolean matches(Object o) {
                Subject subject = (Subject) o;
                return subject != null && subject.getId().equals(apiKey) && subject.getInternalId().equals(internalId);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("API key ").appendText(apiKey);
            }
        };
    }

    private Subject isSubject() {
        return argThat(matchesSubject(APIKEY_DATABUS, INTERNAL_ID_DATABUS));
    }

    private Subject createSubject() {
        Subject subject = mock(Subject.class);
        when(subject.getId()).thenReturn(APIKEY_DATABUS);
        when(subject.getInternalId()).thenReturn(INTERNAL_ID_DATABUS);
        return subject;
    }

    @Test
    public void testPeekPartitionContext() {
        _pcxtv.expect(PartitionContextBuilder.of("queue-name"))
            .peek(createSubject(), "queue-name", 123);
    }

    @Test
    public void testPeekHidden() {
        List<Event> peekResults = ImmutableList.of(
            new Event("id-1", ImmutableMap.of("key-1", "value-1", "~hidden.k", "val1"), ImmutableList.of(ImmutableList.of("tag-1"))),
            new Event("id-2", ImmutableMap.of("key-2", "value-2", "~hidden.k", "val2"), ImmutableList.of(ImmutableList.of("tag-2"))));
        List<Event> peekResultsStripped = ImmutableList.of(
            new Event("id-1", ImmutableMap.of("key-1", "value-1"), ImmutableList.of(ImmutableList.of("tag-1"))),
            new Event("id-2", ImmutableMap.of("key-2", "value-2"), ImmutableList.of(ImmutableList.of("tag-2"))));
        when(_client.peek(isSubject(), eq("queue-name"), eq(123))).thenReturn(peekResults);

        assertEquals(peekResultsStripped, databusClient().peek("queue-name", 123));
        verify(_client).peek(isSubject(), eq("queue-name"), eq(123));
        verifyNoMoreInteractions(_local, _client);
    }

    @Test
    public void testPoll() {
        List<Event> pollResults = ImmutableList.of(
            new Event("id-1", ImmutableMap.of("key-1", "value-1", "~hidden.k", "val1"), ImmutableList.of(ImmutableList.of("tag-1"))),
            new Event("id-2", ImmutableMap.of("key-2", "value-2", "~hidden.k", "val2"), ImmutableList.of(ImmutableList.of("tag-2"))));
        List<Event> pollResultsStripped = ImmutableList.of(
            new Event("id-1", ImmutableMap.of("key-1", "value-1"), ImmutableList.of(ImmutableList.of("tag-1"))),
            new Event("id-2", ImmutableMap.of("key-2", "value-2"), ImmutableList.of(ImmutableList.of("tag-2"))));

        when(_client.poll(isSubject(), eq("queue-name"), eq(Duration.standardSeconds(15)), eq(123)))
            .thenReturn(new PollResult(pollResults, false));

        // This is the default poll behavior
        PollResult pollResult = databusClient().poll("queue-name", Duration.standardSeconds(15), 123);
        assertFalse(pollResult.hasMoreEvents());

        assertEquals(pollResultsStripped, pollResult.getEvents());
        verify(_client).poll(isSubject(), eq("queue-name"), eq(Duration.standardSeconds(15)), eq(123));
        verifyNoMoreInteractions(_local, _client);
    }
}
