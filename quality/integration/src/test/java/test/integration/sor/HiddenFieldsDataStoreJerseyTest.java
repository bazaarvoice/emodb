package test.integration.sor;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.identity.InMemoryAuthIdentityManager;
import com.bazaarvoice.emodb.auth.permissions.InMemoryPermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.common.api.UnauthorizedException;
import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.ChangeBuilder;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.api.Coordinate;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.History;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.client.DataStoreAuthenticator;
import com.bazaarvoice.emodb.sor.client.DataStoreClient;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.core.DataStoreAsync;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.test.ResourceTest;
import com.bazaarvoice.emodb.web.auth.EmoPermissionResolver;
import com.bazaarvoice.emodb.web.resources.sor.DataStoreResource1;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.net.URI;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests the api calls made via the Jersey HTTP client {@link DataStoreClient} are
 * interpreted correctly by the Jersey HTTP resource {@link DataStoreResource1}.
 */
@SuppressWarnings("Duplicates") public class HiddenFieldsDataStoreJerseyTest extends ResourceTest {
    private static final String APIKEY_READ = "read-key";
    private static final String APIKEY_UPDATE = "update-key";
    private final String TABLE = "h-table";

    private DataStore _server = mock(DataStore.class);
    private DataCenters _dataCenters = mock(DataCenters.class);

    @Rule
    public ResourceTestRule _resourceTestRule = setupDataStoreResourceTestRule();

    private ResourceTestRule setupDataStoreResourceTestRule() {
        InMemoryAuthIdentityManager<ApiKey> authIdentityManager = new InMemoryAuthIdentityManager<>();
        authIdentityManager.updateIdentity(new ApiKey(APIKEY_READ, "id6", ImmutableSet.of("read-role")));
        authIdentityManager.updateIdentity(new ApiKey(APIKEY_UPDATE, "id7", ImmutableSet.of("update-role")));

        EmoPermissionResolver permissionResolver = new EmoPermissionResolver(_server, mock(BlobStore.class));
        InMemoryPermissionManager permissionManager = new InMemoryPermissionManager(permissionResolver);
        permissionManager.updateForRole("read-role", new PermissionUpdateRequest().permit("sor|read|*"));
        permissionManager.updateForRole("update-role", new PermissionUpdateRequest().permit("sor|read|*", "sor|update|*"));

        return setupResourceTestRule(Collections.<Object>singletonList(new DataStoreResource1(_server, mock(DataStoreAsync.class))), authIdentityManager, permissionManager);
    }

    @After
    public void tearDownMocksAndClearState() {
        verifyNoMoreInteractions(_server, _dataCenters);
        reset(_server, _dataCenters);
    }

    private DataStore sorClient(String apiKey) {
        return DataStoreAuthenticator.proxied(new DataStoreClient(URI.create("/sor/1"), new JerseyEmoClient(_resourceTestRule.client())))
            .usingCredentials(apiKey);
    }

    private DataStore hiddenFieldsClient(String apiKey) {
        return DataStoreAuthenticator.proxied(new DataStoreClient(URI.create("/sor/1"), new JerseyEmoClient(_resourceTestRule.client()), true))
            .usingCredentials(apiKey);
    }

    @Test
    public void testGetDocHidden() {
        {
            final ImmutableMap<String, Object> storedDoc = ImmutableMap.of("asdf", "qwer", "~hidden.attr1", "value");
            when(_server.get(TABLE, "k", ReadConsistency.STRONG)).thenReturn(storedDoc);
        }
        {
            final Map<String, Object> result = sorClient(APIKEY_READ).get(TABLE, "k");
            assertEquals(ImmutableMap.of("asdf", "qwer"), result);
        }
        {
            try {
                hiddenFieldsClient(APIKEY_READ).get(TABLE, "k");
                fail("should have thrown");
            } catch (Exception e) {
                assertTrue(e instanceof UnauthorizedException);
            }
        }
        {
            final Map<String, Object> result = sorClient(APIKEY_UPDATE).get(TABLE, "k");
            assertEquals(ImmutableMap.of("asdf", "qwer"), result);
        }
        {
            final Map<String, Object> result = hiddenFieldsClient(APIKEY_UPDATE).get(TABLE, "k");
            assertEquals(ImmutableMap.of("asdf", "qwer", "~hidden.attr1", "value"), result);
        }
        verify(_server, times(3)).get(TABLE, "k", ReadConsistency.STRONG);
    }

    @Test
    public void testGetDocTimelineHidden() {
        Audit audit = new AuditBuilder().setLocalHost().build();
        List<Change> expected = ImmutableList.of(
            new ChangeBuilder(TimeUUIDs.newUUID())
                .with(audit)
                .with(new Compaction(
                    1,
                    TimeUUIDs.newUUID(),
                    TimeUUIDs.newUUID(),
                    "",
                    TimeUUIDs.newUUID(),
                    TimeUUIDs.newUUID(),
                    Deltas.literal(ImmutableMap.of("a", 1, "~hidden.b", 2))
                ))
                .with(new History(
                    TimeUUIDs.newUUID(),
                    ImmutableMap.of("e", 5, "~hidden.f", 6),
                    Deltas.literal(ImmutableMap.of("g", 7, "~hidden.h", 8))
                ))
                .build(),
            new ChangeBuilder(TimeUUIDs.newUUID())
                .with(audit)
                .with(Deltas.conditional(
                    Conditions.mapBuilder().contains("c", 3).contains("~hidden.d", 4).build(),
                    Deltas.mapBuilder().put("i", 9).put("~hidden.j", 10).build()
                ))
                .build()
        );

        when(_server.getTimeline(TABLE, "k", false, false, null, null, false, 10L, ReadConsistency.STRONG))
            .thenAnswer(invocation -> expected.iterator());

        {
            final Iterator<Change> result = sorClient(APIKEY_READ).getTimeline(TABLE, "k", false, false, null, null, false, 10L, ReadConsistency.STRONG);
            assertFalse(JsonHelper.asJson(result.next()).contains("~hidden"));
            assertFalse(JsonHelper.asJson(result.next()).contains("~hidden"));
            assertFalse(result.hasNext());
        }
        {
            try {
                final DataStore dataStore = hiddenFieldsClient(APIKEY_READ);
                dataStore.getTimeline(TABLE, "k", false, false, null, null, false, 10L, ReadConsistency.STRONG);
                fail("should have thrown");
            } catch (Exception e) {
                assertTrue(e instanceof UnauthorizedException);
            }
        }
        {
            final Iterator<Change> result = sorClient(APIKEY_UPDATE).getTimeline(TABLE, "k", false, false, null, null, false, 10L, ReadConsistency.STRONG);
            final String firstJson = JsonHelper.asJson(result.next());
            assertFalse(firstJson.contains("~hidden"));
            final String secondJson = JsonHelper.asJson(result.next());
            assertFalse(secondJson.contains("~hidden"));
            assertFalse(result.hasNext());
        }
        {
            final Iterator<Change> result = hiddenFieldsClient(APIKEY_UPDATE).getTimeline(TABLE, "k", false, false, null, null, false, 10L, ReadConsistency.STRONG);
            final String firstJson = JsonHelper.asJson(result.next());
            assertTrue(firstJson.contains("~hidden.b"));
            assertTrue(firstJson.contains("~hidden.f"));
            assertTrue(firstJson.contains("~hidden.h"));
            final String secondJson = JsonHelper.asJson(result.next());
            assertTrue(secondJson.contains("~hidden.d"));
            assertTrue(secondJson.contains("~hidden.j"));
            assertFalse(result.hasNext());
        }
        verify(_server, times(3)).getTimeline(TABLE, "k", false, false, null, null, false, 10L, ReadConsistency.STRONG);
    }

    @Test public void testScanHidden() {
        final Map<String, Object> doc = ImmutableMap.<String, Object>of("asdf", "qwer", "~hidden.f", "value");
        when(_server.scan(TABLE, null, 10, ReadConsistency.STRONG)).thenAnswer(invocation -> ImmutableList.of(doc).iterator());

        {
            final Iterator<Map<String, Object>> scan = sorClient(APIKEY_READ).scan(TABLE, null, 10L, ReadConsistency.STRONG);
            final Map<String, Object> result = scan.next();
            assertFalse(scan.hasNext());
            assertEquals(result.get("asdf"), "qwer");
            assertFalse(result.containsKey("~hidden.f"));
        }
        {
            try {
                hiddenFieldsClient(APIKEY_READ).scan(TABLE, null, 10L, ReadConsistency.STRONG);
                fail("should have thrown");
            } catch (Exception e) {
                assertTrue(e instanceof UnauthorizedException);
            }
        }
        {
            final Iterator<Map<String, Object>> scan = sorClient(APIKEY_UPDATE).scan(TABLE, null, 10L, ReadConsistency.STRONG);
            final Map<String, Object> result = scan.next();
            assertFalse(scan.hasNext());
            assertEquals(result.get("asdf"), "qwer");
            assertFalse(result.containsKey("~hidden.f"));
        }
        {
            final Iterator<Map<String, Object>> scan = hiddenFieldsClient(APIKEY_UPDATE).scan(TABLE, null, 10L, ReadConsistency.STRONG);
            final Map<String, Object> result = scan.next();
            assertFalse(scan.hasNext());
            assertEquals(result.get("asdf"), "qwer");
            assertEquals(result.get("~hidden.f"), "value");
        }
        verify(_server, times(3)).scan(TABLE, null, 10, ReadConsistency.STRONG);
    }

    @Test public void testGetSplitHidden() {
        final Map<String, Object> doc = ImmutableMap.<String, Object>of("asdf", "qwer", "~hidden.f", "value");
        when(_server.getSplit(TABLE, "schplit", null, 10, ReadConsistency.STRONG)).thenAnswer(new Answer<Iterator<Map<String, Object>>>() {
            @Override public Iterator<Map<String, Object>> answer(final InvocationOnMock invocation) throws Throwable {
                return ImmutableList.of(doc).iterator();
            }
        });

        {
            final Iterator<Map<String, Object>> split = sorClient(APIKEY_READ).getSplit(TABLE, "schplit", null, 10, ReadConsistency.STRONG);
            final Map<String, Object> result = split.next();
            assertFalse(split.hasNext());
            assertEquals(result.get("asdf"), "qwer");
            assertFalse(result.containsKey("~hidden.f"));
        }

        {
            try {
                hiddenFieldsClient(APIKEY_READ).getSplit(TABLE, "schplit", null, 10, ReadConsistency.STRONG);
                fail("should have thrown");
            } catch (Exception e) {
                assertTrue(e instanceof UnauthorizedException);
            }
        }


        {
            final Iterator<Map<String, Object>> split = sorClient(APIKEY_UPDATE).getSplit(TABLE, "schplit", null, 10, ReadConsistency.STRONG);
            final Map<String, Object> result = split.next();
            assertFalse(split.hasNext());
            assertEquals(result.get("asdf"), "qwer");
            assertFalse(result.containsKey("~hidden.f"));
        }


        {
            final Iterator<Map<String, Object>> split = hiddenFieldsClient(APIKEY_UPDATE).getSplit(TABLE, "schplit", null, 10, ReadConsistency.STRONG);
            final Map<String, Object> result = split.next();
            assertFalse(split.hasNext());
            assertEquals(result.get("asdf"), "qwer");
            assertEquals(result.get("~hidden.f"), "value");
        }

        verify(_server, times(3)).getSplit(TABLE, "schplit", null, 10, ReadConsistency.STRONG);
    }

    @Test public void testMultiGetRestricted() {
        final Coordinate a1Id = Coordinate.of(TABLE, "asdf");
        final Map<String, Object> a1Doc = ImmutableMap.of("asdf", "ghjk", "~hidden.k", "v1");
        final Map<String, Object> a1DocStrip = ImmutableMap.of("asdf", "ghjk");
        final Coordinate a2Id = Coordinate.of(TABLE, "qwer");
        final Map<String, Object> a2Doc = ImmutableMap.of("qwer", "tyui", "~hidden.k", "v1");
        final Map<String, Object> a2DocStrip = ImmutableMap.of("qwer", "tyui");
        final Coordinate b1Id = Coordinate.of(TABLE, "zxcv");
        final Map<String, Object> b1Doc = ImmutableMap.of("zxcv", "bnm,", "~hidden.k", "v1");
        final Map<String, Object> b1DocStrip = ImmutableMap.of("zxcv", "bnm,");

        when(_server.multiGet(ImmutableList.of(a1Id, a2Id, b1Id), ReadConsistency.STRONG))
            .thenAnswer(invocation -> ImmutableList.of(a1Doc, a2Doc, b1Doc).iterator());

        {
            final Set<Map<String, Object>> result = ImmutableSet.copyOf(sorClient(APIKEY_READ).multiGet(ImmutableList.of(a1Id, a2Id, b1Id), ReadConsistency.STRONG));
            assertEquals(ImmutableSet.of(a1DocStrip, a2DocStrip, b1DocStrip), result);
        }

        {
            try {
                ImmutableSet.copyOf(hiddenFieldsClient(APIKEY_READ).multiGet(ImmutableList.of(a1Id, a2Id, b1Id), ReadConsistency.STRONG));
                fail("should have thrown");
            } catch (Exception e) {
                assertTrue(e instanceof UnauthorizedException);
            }
        }

        {
            final Set<Map<String, Object>> result = ImmutableSet.copyOf(sorClient(APIKEY_UPDATE).multiGet(ImmutableList.of(a1Id, a2Id, b1Id), ReadConsistency.STRONG));
            assertEquals(ImmutableSet.of(a1DocStrip, a2DocStrip, b1DocStrip), result);
        }

        {
            final Set<Map<String, Object>> result = ImmutableSet.copyOf(hiddenFieldsClient(APIKEY_UPDATE).multiGet(ImmutableList.of(a1Id, a2Id, b1Id), ReadConsistency.STRONG));
            assertEquals(ImmutableSet.of(a1Doc, a2Doc, b1Doc), result);
        }

        verify(_server, times(3)).multiGet(ImmutableList.of(a1Id, a2Id, b1Id), ReadConsistency.STRONG);
    }
}
