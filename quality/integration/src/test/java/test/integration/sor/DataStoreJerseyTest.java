package test.integration.sor;

import com.bazaarvoice.emodb.auth.InvalidCredentialException;
import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyModification;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.bazaarvoice.emodb.auth.identity.InMemoryAuthIdentityManager;
import com.bazaarvoice.emodb.auth.permissions.InMemoryPermissionManager;
import com.bazaarvoice.emodb.auth.role.InMemoryRoleManager;
import com.bazaarvoice.emodb.auth.role.RoleManager;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.common.api.UnauthorizedException;
import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.common.json.JsonStreamProcessingException;
import com.bazaarvoice.emodb.common.json.RisonHelper;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.ChangeBuilder;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.api.Coordinate;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.DefaultTable;
import com.bazaarvoice.emodb.sor.api.FacadeOptions;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.Table;
import com.bazaarvoice.emodb.sor.api.TableAvailability;
import com.bazaarvoice.emodb.sor.api.TableExistsException;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.UnknownPlacementException;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.sor.api.Update;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.client.DataStoreAuthenticator;
import com.bazaarvoice.emodb.sor.client.DataStoreClient;
import com.bazaarvoice.emodb.sor.client.DataStoreStreaming;
import com.bazaarvoice.emodb.sor.compactioncontrol.InMemoryCompactionControlSource;
import com.bazaarvoice.emodb.sor.core.DataStoreAsync;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.test.ResourceTest;
import com.bazaarvoice.emodb.web.auth.DefaultRoles;
import com.bazaarvoice.emodb.web.auth.EmoPermissionResolver;
import com.bazaarvoice.emodb.web.resources.sor.DataStoreResource1;
import com.bazaarvoice.emodb.web.throttling.UnlimitedDataStoreUpdateThrottler;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.UnmodifiableIterator;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.Stubber;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Tests the api calls made via the Jersey HTTP client {@link DataStoreClient} are
 * interpreted correctly by the Jersey HTTP resource {@link DataStoreResource1}.
 */
public class DataStoreJerseyTest extends ResourceTest {
    private static final String APIKEY_TABLE = "table-key";
    private static final String APIKEY_READ_TABLES_A = "read-tables-a";
    private static final String APIKEY_READ_TABLES_B = "read-tables-b";
    private static final String APIKEY_FACADE = "facade-key";
    private static final String APIKEY_REVIEWS_ONLY = "reviews-only-key";
    private static final String APIKEY_STANDARD = "standard-key";
    private static final String APIKEY_STANDARD_UPDATE = "standard-update";

    private DataStore _server = mock(DataStore.class);
    private DataCenters _dataCenters = mock(DataCenters.class);

    @Rule
    public ResourceTestRule _resourceTestRule = setupDataStoreResourceTestRule();

    private ResourceTestRule setupDataStoreResourceTestRule() {
        InMemoryAuthIdentityManager<ApiKey> authIdentityManager = new InMemoryAuthIdentityManager<>();
        authIdentityManager.createIdentity(APIKEY_TABLE, new ApiKeyModification().addRoles("table-role"));
        authIdentityManager.createIdentity(APIKEY_READ_TABLES_A, new ApiKeyModification().addRoles("tables-a-role"));
        authIdentityManager.createIdentity(APIKEY_READ_TABLES_B, new ApiKeyModification().addRoles("tables-b-role"));
        authIdentityManager.createIdentity(APIKEY_FACADE, new ApiKeyModification().addRoles("facade-role"));
        authIdentityManager.createIdentity(APIKEY_REVIEWS_ONLY, new ApiKeyModification().addRoles("reviews-only-role"));
        authIdentityManager.createIdentity(APIKEY_STANDARD, new ApiKeyModification().addRoles("standard"));
        authIdentityManager.createIdentity(APIKEY_STANDARD_UPDATE, new ApiKeyModification().addRoles("update-with-events"));

        EmoPermissionResolver permissionResolver = new EmoPermissionResolver(_server, mock(BlobStore.class));
        InMemoryPermissionManager permissionManager = new InMemoryPermissionManager(permissionResolver);
        RoleManager roleManager = new InMemoryRoleManager(permissionManager);

        createRole(roleManager, null, "table-role", ImmutableSet.of("sor|*|*"));
        createRole(roleManager, null, "tables-a-role", ImmutableSet.of("sor|read|a*"));
        createRole(roleManager, null, "tables-b-role", ImmutableSet.of("sor|read|b*"));
        createRole(roleManager, null, "facade-role", ImmutableSet.of("facade|*|*"));
        createRole(roleManager, null, "reviews-only-role", ImmutableSet.of("sor|*|if({..,\"type\":\"review\"})"));
        createRole(roleManager, null, "standard", DefaultRoles.standard.getPermissions());
        createRole(roleManager, null, "update-with-events", ImmutableSet.of("sor|update|*"));

        return setupResourceTestRule(Collections.<Object>singletonList(
                new DataStoreResource1(_server, mock(DataStoreAsync.class), new InMemoryCompactionControlSource(), new UnlimitedDataStoreUpdateThrottler())),
                authIdentityManager, permissionManager);
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

    @Test
    public void testGetDocRestricted() {
        final ImmutableMap<String, Object> doc = ImmutableMap.<String, Object>of("asdf", "qwer");
        when(_server.get("a-table-1", "k", ReadConsistency.STRONG)).thenReturn(doc);
        {
            final Map<String, Object> map = sorClient(APIKEY_READ_TABLES_A).get("a-table-1", "k");
            assertEquals(doc, map);
            verify(_server).get("a-table-1", "k", ReadConsistency.STRONG);
        }
        {
            try {
                sorClient(APIKEY_READ_TABLES_B).get("a-table-1", "k");
                fail("should have thrown");
            } catch (Exception e) {
                assertTrue(e instanceof UnauthorizedException);
            }
        }
    }

    @Test
    public void testGetDocTimelineRestricted() {
        Audit audit = new AuditBuilder().setLocalHost().build();
        List<Change> expected = ImmutableList.of(
                new ChangeBuilder(TimeUUIDs.newUUID()).build());
        when(_server.getTimeline("a-table-1", "k", false, false, null, null, false, 10L, ReadConsistency.STRONG))
                .thenReturn(expected.iterator());

        {
            final Iterator<Change> result = sorClient(APIKEY_READ_TABLES_A).getTimeline("a-table-1", "k", false, false, null, null, false, 10L, ReadConsistency.STRONG);
            final Change actual = result.next();
            assertFalse(result.hasNext());
            assertEquals(expected.get(0).getId(), actual.getId());
            verify(_server).getTimeline("a-table-1", "k", false, false, null, null, false, 10L, ReadConsistency.STRONG);
        }
        {
            try {
                sorClient(APIKEY_READ_TABLES_B).getTimeline("a-table-1", "k", false, false, null, null, false, 10L, ReadConsistency.STRONG);
                fail("should have thrown");
            } catch (Exception e) {
                assertTrue(e instanceof UnauthorizedException);
            }
        }
    }

    @Test public void testScanRestricted() {
        final Map<String, Object> doc = ImmutableMap.<String, Object>of("asdf", "qwer", Intrinsic.DELETED, false);
        when(_server.scan("a-table-1", null, Long.MAX_VALUE, true, ReadConsistency.STRONG)).thenAnswer(new Answer<Iterator<Map<String, Object>>>() {
            @Override public Iterator<Map<String, Object>> answer(final InvocationOnMock invocation) throws Throwable {
                return ImmutableList.of(doc).iterator();
            }
        });

        {
            final Iterator<Map<String, Object>> scan = sorClient(APIKEY_READ_TABLES_A).scan("a-table-1", null, 10L, false, ReadConsistency.STRONG);
            final Map<String, Object> result = scan.next();
            assertFalse(scan.hasNext());
            assertEquals(doc, result);
            verify(_server).scan("a-table-1", null, Long.MAX_VALUE, true, ReadConsistency.STRONG);
        }
        {
            try {
                sorClient(APIKEY_READ_TABLES_B).scan("a-table-1", null, 10L, false, ReadConsistency.STRONG);
                fail("should have thrown");
            } catch (Exception e) {
                assertTrue(e instanceof UnauthorizedException);
            }
        }
    }

    @Test public void testGetSplitsRestricted() {
        final ImmutableList<String> splits = ImmutableList.of("schplit");
        when(_server.getSplits("a-table-1", 10)).thenReturn(splits);

        {
            final Collection<String> result = sorClient(APIKEY_READ_TABLES_A).getSplits("a-table-1", 10);
            assertEquals(result.size(), 1);
            assertEquals(splits.get(0), result.iterator().next());
            verify(_server).getSplits("a-table-1", 10);
        }
        {
            try {
                sorClient(APIKEY_READ_TABLES_B).getSplits("a-table-1", 10);
                fail("should have thrown");
            } catch (Exception e) {
                assertTrue(e instanceof UnauthorizedException);
            }
        }
    }

    @Test public void testGetSplitRestricted() {
        final Map<String, Object> doc = ImmutableMap.<String, Object>of("asdf", "qwer", Intrinsic.DELETED, false);
        when(_server.getSplit("a-table-1", "schplit", null, Long.MAX_VALUE, true, ReadConsistency.STRONG)).thenAnswer(new Answer<Iterator<Map<String, Object>>>() {
            @Override public Iterator<Map<String, Object>> answer(final InvocationOnMock invocation) throws Throwable {
                return ImmutableList.of(doc).iterator();
            }
        });

        {
            final Iterator<Map<String, Object>> split = sorClient(APIKEY_READ_TABLES_A).getSplit("a-table-1", "schplit", null, 10, false, ReadConsistency.STRONG);
            final Map<String, Object> result = split.next();
            assertFalse(split.hasNext());
            assertEquals(doc, result);
            verify(_server).getSplit("a-table-1", "schplit", null, Long.MAX_VALUE, true, ReadConsistency.STRONG);
        }

        {
            try {
                sorClient(APIKEY_READ_TABLES_B).getSplit("a-table-1", "schplit", null, 10, false, ReadConsistency.STRONG);
                fail("should have thrown");
            } catch (Exception e) {
                assertTrue(e instanceof UnauthorizedException);
            }
        }
    }

    @Test public void testMultiGetRestricted() {

        final Coordinate a1Id = Coordinate.of("a-table-1", "asdf");
        final Map<String, Object> a1Doc = ImmutableMap.<String, Object>of("asdf", "ghjk");
        final Coordinate a2Id = Coordinate.of("a-table-2", "qwer");
        final Map<String, Object> a2Doc = ImmutableMap.<String, Object>of("qwer", "tyui");
        final Coordinate b1Id = Coordinate.of("b-table-1", "zxcv");
        final Map<String, Object> b1Doc = ImmutableMap.<String, Object>of("zxcv", "bnm,");

        when(_server.multiGet(ImmutableList.of(a1Id, a2Id), ReadConsistency.STRONG))
                .thenAnswer(new Answer<Iterator<Map<String, Object>>>() {
                    @Override public Iterator<Map<String, Object>> answer(final InvocationOnMock invocation) throws Throwable {
                        return ImmutableList.of(a1Doc, a2Doc).iterator();
                    }
                });

        when(_server.multiGet(ImmutableList.of(b1Id), ReadConsistency.STRONG))
                .thenAnswer(new Answer<Iterator<Map<String, Object>>>() {
                    @Override public Iterator<Map<String, Object>> answer(final InvocationOnMock invocation) throws Throwable {
                        return ImmutableList.of(b1Doc).iterator();
                    }
                });

        when(_server.multiGet(ImmutableList.of(a1Id, a2Id, b1Id), ReadConsistency.STRONG))
                .thenAnswer(new Answer<Iterator<Map<String, Object>>>() {
                    @Override public Iterator<Map<String, Object>> answer(final InvocationOnMock invocation) throws Throwable {
                        return ImmutableList.of(a1Doc, a2Doc, b1Doc).iterator();
                    }
                });

        {
            final Set<Map<String, Object>> result = ImmutableSet.copyOf(sorClient(APIKEY_READ_TABLES_A).multiGet(ImmutableList.of(a1Id, a2Id), ReadConsistency.STRONG));
            assertEquals(ImmutableSet.of(a1Doc, a2Doc), result);
            verify(_server).multiGet(ImmutableList.of(a1Id, a2Id), ReadConsistency.STRONG);
        }

        {
            final Set<Map<String, Object>> result = ImmutableSet.copyOf(sorClient(APIKEY_READ_TABLES_B).multiGet(ImmutableList.of(b1Id), ReadConsistency.STRONG));
            assertEquals(ImmutableSet.of(b1Doc), result);
            verify(_server).multiGet(ImmutableList.of(b1Id), ReadConsistency.STRONG);
        }

        {
            try {
                ImmutableSet.copyOf(sorClient(APIKEY_READ_TABLES_A).multiGet(ImmutableList.of(a1Id, a2Id, b1Id), ReadConsistency.STRONG));
                fail("should have thrown");
            } catch (Exception e) {
                assertTrue(e instanceof UnauthorizedException);
            }
        }

        {
            try {
                ImmutableSet.copyOf(sorClient(APIKEY_READ_TABLES_B).multiGet(ImmutableList.of(a1Id, a2Id, b1Id), ReadConsistency.STRONG));
                fail("should have thrown");
            } catch (Exception e) {
                assertTrue(e instanceof UnauthorizedException);
            }
        }
    }

    @Test
    public void testListTablesRestricted() {
        final TableOptions options = new TableOptionsBuilder().setPlacement("my:placement").build();
        final ImmutableMap<String, Object> template = ImmutableMap.<String, Object>of("key", "value1");
        final TableAvailability availability = new TableAvailability("my:placement", false);
        final DefaultTable a1 = new DefaultTable("a-table-1", options, template, availability);
        final DefaultTable a2 = new DefaultTable("a-table-2", options, template, availability);
        final DefaultTable b1 = new DefaultTable("b-table-1", options, template, availability);
        final DefaultTable b2 = new DefaultTable("b-table-2", options, template, availability);
        final DefaultTable a3 = new DefaultTable("a-table-3", options, template, availability);
        final ImmutableList<Table> tables = ImmutableList.of(a1, a2, b1, b2, a3);

        final UnmodifiableIterator<Table> iterator = tables.iterator();
        //noinspection unchecked
        when(_server.listTables(null, Long.MAX_VALUE)).thenAnswer(invocation -> iterator);

        {
            final Iterator<Table> tableIterator = sorClient(APIKEY_READ_TABLES_A).listTables(null, 3);
            final ImmutableList<Table> result = ImmutableList.copyOf(tableIterator);
            assertEquals(ImmutableList.<Table>of(a1, a2, a3), result);
        }
        verify(_server, times(1)).listTables(null, Long.MAX_VALUE);
    }

    @Test public void testGetTableTemplateRestricted() {
        final ImmutableMap<String, Object> template = ImmutableMap.<String, Object>of("key", "value1");
        when(_server.getTableTemplate("a-table-1")).thenReturn(template);
        {
            final Map<String, Object> result = sorClient(APIKEY_READ_TABLES_A).getTableTemplate("a-table-1");
            assertEquals(template, result);
            verify(_server).getTableTemplate("a-table-1");
        }
        {
            try {
                sorClient(APIKEY_READ_TABLES_B).getTableTemplate("a-table-1");
                fail("unreachable");
            } catch (Exception e) {
                assertTrue(e instanceof UnauthorizedException);
            }
        }
    }

    @Test public void testGetTableOptionsRestricted() {
        final TableOptions options = new TableOptionsBuilder().setPlacement("my:placement").build();
        when(_server.getTableOptions("a-table-1")).thenReturn(options);
        {
            final TableOptions result = sorClient(APIKEY_READ_TABLES_A).getTableOptions("a-table-1");
            assertEquals(options, result);
            verify(_server).getTableOptions("a-table-1");
        }
        {
            try {
                sorClient(APIKEY_READ_TABLES_B).getTableOptions("a-table-1");
                fail("unreachable");
            } catch (Exception e) {
                assertTrue(e instanceof UnauthorizedException);
            }
        }
    }

    @Test public void testGetTableSizeRestricted() {
        final long size = 3L;
        when(_server.getTableApproximateSize("a-table-1")).thenReturn(size);
        {
            final long result = sorClient(APIKEY_READ_TABLES_A).getTableApproximateSize("a-table-1");
            assertEquals(size, result);
            verify(_server).getTableApproximateSize("a-table-1");
        }
        {
            try {
                sorClient(APIKEY_READ_TABLES_B).getTableApproximateSize("a-table-1");
                fail("unreachable");
            } catch (Exception e) {
                assertTrue(e instanceof UnauthorizedException);
            }
        }
    }

    @Test public void testGetTableSizeLimitedRestricted() {
        final long size = 3L;
        when(_server.getTableApproximateSize("a-table-1", 5)).thenReturn(size);
        {
            final long result = sorClient(APIKEY_READ_TABLES_A).getTableApproximateSize("a-table-1", 5);
            assertEquals(size, result);
            verify(_server).getTableApproximateSize("a-table-1", 5);
        }
        {
            try {
                sorClient(APIKEY_READ_TABLES_B).getTableApproximateSize("a-table-1", 5);
                fail("unreachable");
            } catch (Exception e) {
                assertTrue(e instanceof UnauthorizedException);
            }
        }
    }

    @Test public void testGetTableMetadataRestricted() {
        final TableOptions options = new TableOptionsBuilder().setPlacement("my:placement").build();
        final TableAvailability availability = new TableAvailability("my:placement", false);
        final Table metadata = new DefaultTable("table-1", options, ImmutableMap.<String, Object>of("key", "value1"), availability);
        when(_server.getTableMetadata("a-table-1")).thenReturn(metadata);
        {
            final Table result = sorClient(APIKEY_READ_TABLES_A).getTableMetadata("a-table-1");
            assertEquals(metadata, result);
            verify(_server).getTableMetadata("a-table-1");
        }
        {
            try {
                sorClient(APIKEY_READ_TABLES_B).getTableMetadata("a-table-1");
                fail("unreachable");
            } catch (Exception e) {
                assertTrue(e instanceof UnauthorizedException);
            }
        }
    }

    @Test
    public void testListTables() {
        TableOptions options = new TableOptionsBuilder().setPlacement("my:placement").build();
        TableAvailability availability = new TableAvailability("my:placement", false);
        List<Table> expected = ImmutableList.<Table>of(
                new DefaultTable("table-1", options, ImmutableMap.<String, Object>of("key", "value1"), availability),
                new DefaultTable("table-2", options, ImmutableMap.<String, Object>of("key", "value2"), availability));
        when(_server.listTables(null, Long.MAX_VALUE)).thenReturn(expected.iterator());

        List<Table> actual = Lists.newArrayList(
                DataStoreStreaming.listTables(sorClient(APIKEY_TABLE))
        );

        assertEquals(actual, expected);
        verify(_server).listTables(null, Long.MAX_VALUE);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testListTablesFrom() {
        TableOptions options = new TableOptionsBuilder().setPlacement("my:placement").build();
        TableAvailability availability = new TableAvailability("my:placement", false);
        List<Table> expected = ImmutableList.<Table>of(
                new DefaultTable("table-1", options, ImmutableMap.<String, Object>of("key", "value1"), availability),
                new DefaultTable("table-2", options, ImmutableMap.<String, Object>of("key", "value2"), availability));
        when(_server.listTables("from-key", Long.MAX_VALUE)).thenReturn(expected.iterator());

        List<Table> actual = Lists.newArrayList(
                DataStoreStreaming.listTables(sorClient(APIKEY_TABLE), "from-key", 1234L));

        assertEquals(actual, expected);
        verify(_server).listTables("from-key", Long.MAX_VALUE);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testCreateTableWithPermission() {
        testCreateTableWithPermission(APIKEY_REVIEWS_ONLY);
    }

    private void testCreateTableWithPermission(String apiKey) {
        TableOptions options = new TableOptionsBuilder().setPlacement("my:placement").build();
        Map<String, String> attributes = ImmutableMap.of("type", "review");
        Audit audit = new AuditBuilder().setLocalHost().build();
        sorClient(apiKey).createTable("table-name", options, attributes, audit);

        verify(_server).createTable("table-name", options, attributes, audit);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testCreateTableWithoutPermission() {
        testCreateTableWithoutPermission(APIKEY_REVIEWS_ONLY);
    }

    private void testCreateTableWithoutPermission(String apiKey) {
        TableOptions options = new TableOptionsBuilder().setPlacement("my:placement").build();
        Map<String, String> attributes = ImmutableMap.of("type", "not_permitted");
        Audit audit = new AuditBuilder().setLocalHost().build();
        try {
            sorClient(apiKey).createTable("table-name", options, attributes, audit);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof UnauthorizedException);
        }

        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetStashRoot() {
        URI expectedStashURI = URI.create("s3://emodb-us-east-1/stash/ci");
        when(_server.getStashRoot()).thenReturn(expectedStashURI);
        URI actualStashURI = sorClient(APIKEY_STANDARD).getStashRoot();
        assertEquals(actualStashURI, expectedStashURI);
        verify(_server, times(1)).getStashRoot();
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testCreateTable() {
        TableOptions options = new TableOptionsBuilder().setPlacement("my:placement").build();
        Map<String, String> attributes = ImmutableMap.of("key", "value");
        Audit audit = new AuditBuilder().setLocalHost().build();
        sorClient(APIKEY_TABLE).createTable("table-name", options, attributes, audit);

        verify(_server).createTable("table-name", options, attributes, audit);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testCreateFacade() {
        FacadeOptions options = new FacadeOptions("my:placement");
        Audit audit = new AuditBuilder().setLocalHost().build();
        sorClient(APIKEY_FACADE).createFacade("table-name", options, audit);

        verify(_server).createFacade("table-name", options, audit);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testDropTable() {
        DataCenter dataCenter = mock(DataCenter.class);
        when(dataCenter.getName()).thenReturn("mock-datacenter");
        when(_dataCenters.getSelf()).thenReturn(dataCenter);
        when(dataCenter.isSystem()).thenReturn(true);

        Audit audit = new AuditBuilder().setLocalHost().build();
        sorClient(APIKEY_TABLE).dropTable("table-name", audit);

        verify(_server).dropTable("table-name", audit);
        verify(_dataCenters, never()).getSelf();
        verifyNoMoreInteractions(_server, _dataCenters);
    }

    @Test
    public void testDropTableForbidden() {
        DataCenter dataCenter = mock(DataCenter.class);
        when(dataCenter.getName()).thenReturn("mock-datacenter");
        when(_dataCenters.getSelf()).thenReturn(dataCenter);
        when(dataCenter.isSystem()).thenReturn(true);

        TableAvailability availability = new TableAvailability("ugc_global:ugc", false);
        TableOptions options = new TableOptionsBuilder().setPlacement("ugc_global:ugc").build();
        when(_server.getTableMetadata("table-name")).thenReturn(
                new DefaultTable("table-name", options, ImmutableMap.<String, Object>of(), availability));

        Audit audit = new AuditBuilder().setLocalHost().build();

        try {
            sorClient(APIKEY_REVIEWS_ONLY).dropTable("table-name", audit);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof UnauthorizedException);
        }

        verify(_server).getTableMetadata("table-name");
        verifyNoMoreInteractions(_server, _dataCenters);
    }

    @Test
    public void testDropUnknownTableWithRestrictedPermission() {
        // This test requires some explanation.  Dropping an unknown table should return UnknownTableException.
        // However, if the table does not exist then validating permissions will also yield that exception.
        // This test verifies that 404 is returned and not 403 (forbidden).
        DataCenter dataCenter = mock(DataCenter.class);
        when(dataCenter.getName()).thenReturn("mock-datacenter");
        when(_dataCenters.getSelf()).thenReturn(dataCenter);
        when(dataCenter.isSystem()).thenReturn(true);

        when(_server.getTableMetadata("table-name")).thenThrow(new UnknownTableException());
        doThrow(new UnknownTableException()).when(_server).dropTable(eq("table-name"), any(Audit.class));

        Audit audit = new AuditBuilder().setLocalHost().build();

        try {
            // The reviews-only key will cause the table attributes to be validated.
            sorClient(APIKEY_REVIEWS_ONLY).dropTable("table-name", audit);
            fail();
        } catch (UnknownTableException e) {
            // Ok
        }

        verify(_server).getTableMetadata("table-name");
        verifyNoMoreInteractions(_server, _dataCenters);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDropFacade() {
        Audit audit = new AuditBuilder().setLocalHost().build();
        sorClient(APIKEY_TABLE).dropFacade("table-name", "mock-placement", audit);
    }

    @Test
    public void testUpdateAllForFacade() {
        Audit audit = new AuditBuilder().setLocalHost().build();
        ImmutableList<Update> updates = ImmutableList.of(
                new Update("table-name1", "row-key1", TimeUUIDs.newUUID(), Deltas.literal("hello world"), audit),
                new Update("table-name2", "row-key2", TimeUUIDs.newUUID(), Deltas.literal("hello world"), audit));

        List<Update> actualUpdates = Lists.newArrayList();
        //noinspection unchecked
        doUpdateInto(actualUpdates).when(_server).updateAllForFacade(any(Iterable.class), anySet());

        DataStoreStreaming.updateAllForFacade(sorClient(APIKEY_FACADE), updates);

        //noinspection unchecked
        verify(_server).updateAllForFacade(any(Iterable.class), anySet());
        assertEquals(actualUpdates, updates);
        verifyNoMoreInteractions(_server);
    }

    /** Call {@code updateAllForFacade()} with an API key that doesn't have permission to update facades. */
    @Test
    public void testUpdateAllForFacadeForbidden() {

        Audit audit = new AuditBuilder().setLocalHost().build();
        ImmutableList<Update> updates = ImmutableList.of(
                new Update("table-name1", "row-key1", TimeUUIDs.newUUID(), Deltas.literal("hello world"), audit),
                new Update("table-name2", "row-key2", TimeUUIDs.newUUID(), Deltas.literal("hello world"), audit));

        //noinspection unchecked
        doUpdateInto(Lists.newArrayList()).when(_server).updateAllForFacade(any(Iterable.class), anySet());

        DataStore client = sorClient(APIKEY_TABLE);
        try {
            client.updateAllForFacade(updates);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof UnauthorizedException);
        }

        //noinspection unchecked
        verify(_server).updateAllForFacade(any(Iterable.class), anySet());
        verifyNoMoreInteractions(_server);
    }

    private Stubber doUpdateInto(final List<Update> updates) {
        return doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation)
                    throws Throwable {
                //noinspection unchecked
                Iterables.addAll(updates, (Iterable<Update>) invocation.getArguments()[0]);
                return null;
            }
        });
    }

    private Stubber captureUpdatesAndTags(final List<Update> updates, final Set<String> tags) {
        return doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation)
                    throws Throwable {
                //noinspection unchecked
                Iterables.addAll(updates, (Iterable<Update>) invocation.getArguments()[0]);
                //noinspection unchecked
                tags.addAll((Collection<? extends String>) invocation.getArguments()[1]);
                return null;
            }
        });
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPurgeTableUnsafe() {
        Audit audit = new AuditBuilder().setLocalHost().build();
        sorClient(APIKEY_TABLE).purgeTableUnsafe("table-name", audit);
    }

    @Test
    public void testGetTableExists() {
        boolean expected = true;
        when(_server.getTableTemplate("table-name")).thenReturn(ImmutableMap.<String, Object>of());

        boolean actual = sorClient(APIKEY_TABLE).getTableExists("table-name");

        assertEquals(actual, expected);
        verify(_server).getTableTemplate("table-name");
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetTableNotExists() {
        boolean expected = false;
        when(_server.getTableTemplate("table-name")).thenThrow(new UnknownTableException());

        boolean actual = sorClient(APIKEY_TABLE).getTableExists("table-name");

        assertEquals(actual, expected);
        verify(_server).getTableTemplate("table-name");
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetTableAvailable() {
        boolean expected = true;
        TableAvailability availability = new TableAvailability("my:placement", false);
        TableOptions options = new TableOptionsBuilder().setPlacement("my:placement").build();
        when(_server.getTableMetadata("table-name")).thenReturn(
                new DefaultTable("table-1", options, ImmutableMap.<String, Object>of("key", "value1"), availability)
        );

        boolean actual = sorClient(APIKEY_TABLE).isTableAvailable("table-name");

        assertEquals(actual, expected);
        verify(_server).getTableMetadata("table-name");
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetTableNotAvailable() {
        boolean expected = false;
        TableAvailability availability = null;
        TableOptions options = new TableOptionsBuilder().setPlacement("my:placement").build();
        when(_server.getTableMetadata("table-name")).thenReturn(
                new DefaultTable("table-1", options, ImmutableMap.<String, Object>of("key", "value1"), availability)
        );

        boolean actual = sorClient(APIKEY_TABLE).isTableAvailable("table-name");

        assertEquals(actual, expected);
        verify(_server).getTableMetadata("table-name");
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetTableTemplate() {
        Map<String, Object> expected = ImmutableMap.<String, Object>of("key", "value");
        when(_server.getTableTemplate("table-name")).thenReturn(expected);

        Map<String, Object> actual = sorClient(APIKEY_TABLE).getTableTemplate("table-name");

        assertEquals(actual, expected);
        verify(_server).getTableTemplate("table-name");
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testSetTableTemplate() {
        Audit audit = new AuditBuilder().setLocalHost().build();
        sorClient(APIKEY_TABLE).setTableTemplate("table-name", ImmutableMap.<String, Object>of("key", "value"), audit);

        verify(_server).setTableTemplate("table-name", ImmutableMap.<String, Object>of("key", "value"), audit);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetTableOptions() {
        TableOptions expected = new TableOptionsBuilder().setPlacement("my:placement").build();
        when(_server.getTableOptions("table-name")).thenReturn(expected);

        TableOptions actual = sorClient(APIKEY_TABLE).getTableOptions("table-name");

        assertEquals(actual, expected);
        verify(_server).getTableOptions("table-name");
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetTableApproximateSize() {
        long expected = 1234L;
        when(_server.getTableApproximateSize("table-name")).thenReturn(expected);

        long tableApproximateSize = sorClient(APIKEY_TABLE).getTableApproximateSize("table-name");
        assertEquals(tableApproximateSize, expected);

        verify(_server).getTableApproximateSize("table-name");
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetTableApproximateSizeLimit() {
        long expected = 10L;
        when(_server.getTableApproximateSize("table-name", 10)).thenReturn(expected);

        long tableApproximateSize = sorClient(APIKEY_TABLE).getTableApproximateSize("table-name", 10);
        assertEquals(tableApproximateSize, expected);

        verify(_server).getTableApproximateSize("table-name", 10);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGet() {
        Map<String, Object> expected = ImmutableMap.<String, Object>of("key", "value", "count", 1234);
        when(_server.get("table-name", "row-key", ReadConsistency.STRONG)).thenReturn(expected);

        Map<String, Object> actual = sorClient(APIKEY_TABLE).get("table-name", "row-key");

        assertEquals(actual, expected);
        verify(_server).get("table-name", "row-key", ReadConsistency.STRONG);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetWithConsistency() {
        Map<String, Object> expected = ImmutableMap.<String, Object>of("key", "value", "count", 1234);
        when(_server.get("table-name", "row-key", ReadConsistency.WEAK)).thenReturn(expected);

        Map<String, Object> actual = sorClient(APIKEY_TABLE).get("table-name", "row-key", ReadConsistency.WEAK);

        assertEquals(actual, expected);
        verify(_server).get("table-name", "row-key", ReadConsistency.WEAK);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetTimeline() throws Exception {
        UUID start = TimeUUIDs.uuidForTimestamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse("2012-03-15 06:12:34.567"));
        UUID end = TimeUUIDs.uuidForTimestamp(new Date());
        List<Change> expected = ImmutableList.of(
                new ChangeBuilder(TimeUUIDs.newUUID()).build());
        when(_server.getTimeline("table-name", "row-key", false, true, start, end, false, 123, ReadConsistency.WEAK))
                .thenReturn(expected.iterator());

        List<Change> actual = Lists.newArrayList(
                sorClient(APIKEY_TABLE).getTimeline("table-name", "row-key", false, true, start, end, false, 123, ReadConsistency.WEAK));

        assertEquals(actual.size(), expected.size());
        assertEquals(actual.get(0).getId(), expected.get(0).getId());
        assertEquals(actual.get(0).getDelta(), expected.get(0).getDelta());
        assertEquals(actual.get(0).getCompaction(), expected.get(0).getCompaction());
        verify(_server).getTimeline("table-name", "row-key", false, true, start, end, false, 123, ReadConsistency.WEAK);
        verifyNoMoreInteractions(_server);
    }

    /** Test getTimeline(), omitting all optional query parameters. */
    @Test
    public void testGetTimelineRESTDefault() throws Exception {
        List<Change> expected = ImmutableList.of(
                new ChangeBuilder(TimeUUIDs.newUUID())
                        .with(Deltas.literal("hello world"))
                        .with(new Compaction(5, TimeUUIDs.newUUID(), TimeUUIDs.newUUID(), "1234567890abcdef", null, null))
                        .build());
        when(_server.getTimeline("table-name", "row-key", true, false, null, null, true, 10, ReadConsistency.STRONG))
                .thenReturn(expected.iterator());

        URI uri = UriBuilder.fromUri("/sor/1")
                .segment("table-name", "row-key", "timeline")
                .build();
        List<Change> actual = _resourceTestRule.client().target(uri)
                .request()
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .header(ApiKeyRequest.AUTHENTICATION_HEADER, APIKEY_TABLE)
                .get(new GenericType<List<Change>>() {
                });

        assertEquals(actual.size(), expected.size());
        assertEquals(actual.get(0).getId(), expected.get(0).getId());
        assertEquals(actual.get(0).getDelta(), expected.get(0).getDelta());
        assertEquals(actual.get(0).getCompaction(), expected.get(0).getCompaction());
        verify(_server).getTimeline("table-name", "row-key", true, false, null, null, true, 10, ReadConsistency.STRONG);
        verifyNoMoreInteractions(_server);
    }

    /** Test getTimeline() with timestamp start/end instead of UUIDs. */
    @Test
    public void testGetTimelineRESTTimestampsForward() throws Exception {
        Date start = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse("2012-03-15 16:12:34.567");
        Date end = new Date();
        UUID startUuid = TimeUUIDs.uuidForTimestamp(start);
        UUID endUuid = TimeUUIDs.getPrevious(TimeUUIDs.uuidForTimeMillis(end.getTime() + 1));
        when(_server.getTimeline("table-name", "row-key", true, false, startUuid, endUuid, false, 10, ReadConsistency.STRONG))
                .thenReturn(Collections.emptyIterator());

        DateTimeFormatter format = DateTimeFormatter.ISO_INSTANT;
        URI uri = UriBuilder.fromUri("/sor/1")
                .segment("table-name", "row-key", "timeline")
                .queryParam("start", format.format(start.toInstant()))
                .queryParam("end", format.format(end.toInstant()))
                .queryParam("reversed", "false")
                .build();
        _resourceTestRule.client().target(uri)
                .request()
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .header(ApiKeyRequest.AUTHENTICATION_HEADER, APIKEY_TABLE)
                .get(new GenericType<List<Change>>() {
                });

        verify(_server).getTimeline("table-name", "row-key", true, false, startUuid, endUuid, false, 10, ReadConsistency.STRONG);
        verifyNoMoreInteractions(_server);
    }

    /** Test getTimeline() with timestamp start/end instead of UUIDs. */
    @Test
    public void testGetTimelineRESTTimestampsReversed() throws Exception {
        Date start = new Date();
        Date end = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse("2012-03-15 16:12:34.567");
        UUID startUuid = TimeUUIDs.getPrevious(TimeUUIDs.uuidForTimeMillis(start.getTime() + 1));
        UUID endUuid = TimeUUIDs.uuidForTimestamp(end);
        when(_server.getTimeline("table-name", "row-key", true, false, startUuid, endUuid, true, 10, ReadConsistency.STRONG))
                .thenReturn(Collections.emptyIterator());

        DateTimeFormatter format = DateTimeFormatter.ISO_INSTANT;
        URI uri = UriBuilder.fromUri("/sor/1")
                .segment("table-name", "row-key", "timeline")
                .queryParam("start", format.format(start.toInstant()))
                .queryParam("end", format.format(end.toInstant()))
                .build();
        _resourceTestRule.client().target(uri)
                .request()
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .header(ApiKeyRequest.AUTHENTICATION_HEADER, APIKEY_TABLE)
                .get(new GenericType<List<Change>>() {
                });

        verify(_server).getTimeline("table-name", "row-key", true, false, startUuid, endUuid, true, 10, ReadConsistency.STRONG);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testScan() {
        List<Map<String, Object>> expected = ImmutableList.<Map<String, Object>>of(
                ImmutableMap.<String, Object>of(Intrinsic.ID, "key1", Intrinsic.DELETED, false, "count", 1234),
                ImmutableMap.<String, Object>of(Intrinsic.ID, "key2", Intrinsic.DELETED, false, "count", 5678));
        when(_server.scan("table-name", null, Long.MAX_VALUE, true, ReadConsistency.WEAK)).thenReturn(expected.iterator());

        List<Map<String, Object>> actual = Lists.newArrayList(
                DataStoreStreaming.scan(sorClient(APIKEY_TABLE), "table-name", false, ReadConsistency.WEAK));

        assertEquals(actual, expected);
        verify(_server).scan("table-name", null, Long.MAX_VALUE, true, ReadConsistency.WEAK);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testScanFrom() {
        List<Map<String, Object>> expected = ImmutableList.<Map<String, Object>>of(
                ImmutableMap.<String, Object>of(Intrinsic.ID, "key1", Intrinsic.DELETED, false, "count", 1234),
                ImmutableMap.<String, Object>of(Intrinsic.ID, "key2", Intrinsic.DELETED, false, "count", 5678));
        when(_server.scan("table-name", "from-key", Long.MAX_VALUE, true, ReadConsistency.STRONG)).thenReturn(expected.iterator());

        List<Map<String, Object>> actual = Lists.newArrayList(
                DataStoreStreaming.scan(sorClient(APIKEY_TABLE), "table-name", "from-key", 9876L, false, ReadConsistency.STRONG));

        assertEquals(actual, expected);
        verify(_server).scan("table-name", "from-key", Long.MAX_VALUE, true, ReadConsistency.STRONG);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testScanUnknownTable() {
        try {
            when(_server.scan("non-existent-table", null, 100, true, ReadConsistency.STRONG)).thenThrow(new UnknownTableException());
            Iterable<Map<String, Object>> entries =
                    DataStoreStreaming.scan(sorClient(APIKEY_TABLE), "non-existent-table", null, 100, true, ReadConsistency.STRONG);
            Iterables.getLast(entries);
            fail("UnknownTableException not thrown");
        } catch (UnknownTableException e) {
            // ok
        }
        verify(_server).scan("non-existent-table", null, 100, true, ReadConsistency.STRONG);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetSplits() {
        Collection<String> expected = ImmutableList.of("split-1", "split-2");
        when(_server.getSplits("table-name", 6543)).thenReturn(expected);

        Collection<String> actual = sorClient(APIKEY_TABLE).getSplits("table-name", 6543);

        assertEquals(actual, expected);
        verify(_server).getSplits("table-name", 6543);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetSplit() {
        List<Map<String, Object>> expected = ImmutableList.<Map<String, Object>>of(
                ImmutableMap.<String, Object>of(Intrinsic.ID, "key1", Intrinsic.DELETED, false, "count", 1234),
                ImmutableMap.<String, Object>of(Intrinsic.ID, "key2", Intrinsic.DELETED, false, "count", 5678));
        when(_server.getSplit("table-name", "split-name", null, Long.MAX_VALUE, true, ReadConsistency.WEAK)).thenReturn(expected.iterator());

        List<Map<String, Object>> actual = Lists.newArrayList(
                DataStoreStreaming.getSplit(sorClient(APIKEY_TABLE), "table-name", "split-name", false, ReadConsistency.WEAK));

        assertEquals(actual, expected);
        verify(_server).getSplit("table-name", "split-name", null, Long.MAX_VALUE, true, ReadConsistency.WEAK);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetSplitFrom() {
        List<Map<String, Object>> expected = ImmutableList.<Map<String, Object>>of(
                ImmutableMap.<String, Object>of(Intrinsic.ID, "key1", Intrinsic.DELETED, false, "count", 1234),
                ImmutableMap.<String, Object>of(Intrinsic.ID, "key2", Intrinsic.DELETED, false, "count", 5678));
        when(_server.getSplit("table-name", "split-name", "from-key", Long.MAX_VALUE, true, ReadConsistency.STRONG)).thenReturn(expected.iterator());

        List<Map<String, Object>> actual = Lists.newArrayList(
                DataStoreStreaming.getSplit(sorClient(APIKEY_TABLE), "table-name", "split-name", "from-key", 9876L, false, ReadConsistency.STRONG));

        assertEquals(actual, expected);
        verify(_server).getSplit("table-name", "split-name", "from-key", Long.MAX_VALUE, true, ReadConsistency.STRONG);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetSplitWithMostContentDeleted() {
        List<Map<String, Object>> content = ImmutableList.<Map<String, Object>>of(
                ImmutableMap.of(Intrinsic.ID, "key1", Intrinsic.DELETED, false, "count", 1),
                ImmutableMap.of(Intrinsic.ID, "key2", Intrinsic.DELETED, true, "count", 2),
                ImmutableMap.of(Intrinsic.ID, "key3", Intrinsic.DELETED, true, "count", 3),
                ImmutableMap.of(Intrinsic.ID, "key4", Intrinsic.DELETED, true, "count", 4),
                ImmutableMap.of(Intrinsic.ID, "key5", Intrinsic.DELETED, false, "count", 5),
                ImmutableMap.of(Intrinsic.ID, "key6", Intrinsic.DELETED, false, "count", 6));

        // Create an iterator which delays 100ms before each deleted value
        Iterator<Map<String, Object>> slowIterator = Iterators.filter(content.iterator(), t -> {
            if (Intrinsic.isDeleted(t)) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                }
            }
            return true; });

        when(_server.getSplit("table-name", "split-name", null, Long.MAX_VALUE, true, ReadConsistency.STRONG)).thenReturn(slowIterator);

        // We need to examine the actual JSON response, so call the API directly
        String response = _resourceTestRule.client().target("/sor/1/_split/table-name/split-name")
                .queryParam("limit", "2")
                .queryParam("includeDeletes", "false")
                .queryParam("consistency", ReadConsistency.STRONG.toString())
                .request()
                .header(ApiKeyRequest.AUTHENTICATION_HEADER, APIKEY_TABLE)
                .get(String.class);

        List<Map<String, Object>> actual = JsonHelper.fromJson(response, new TypeReference<List<Map<String, Object>>>() {});

        assertEquals(actual, ImmutableList.of(content.get(0), content.get(4)));
        verify(_server).getSplit("table-name", "split-name", null, Long.MAX_VALUE, true, ReadConsistency.STRONG);
        verifyNoMoreInteractions(_server);

        // Because there was at least 200ms delay between deleted keys 2-4 there should be at least 2 whitespaces between
        // the results which would otherwise not be present.
        int endOfFirstEntry = response.indexOf('}') + 1;
        int commaBeforeSecondEntry = response.indexOf(',', endOfFirstEntry);
        assertTrue(commaBeforeSecondEntry - endOfFirstEntry >= 2);
        assertEquals(Strings.repeat(" ", commaBeforeSecondEntry - endOfFirstEntry), response.substring(endOfFirstEntry, commaBeforeSecondEntry));
    }

    @Test
    public void testMultiGet() {
        List<Map<String, Object>> expected = ImmutableList.<Map<String, Object>>of(
                ImmutableMap.<String, Object>of(Intrinsic.TABLE, "testtable1", Intrinsic.ID, "key1", "count", 1234),
                ImmutableMap.<String, Object>of(Intrinsic.TABLE, "testtable2", Intrinsic.ID, "key&2", "count", 5678));
        List<Coordinate> coordinates = Lists.newArrayList(Coordinate.of("testtable", "key1"),
                Coordinate.of("testtable2", "key&2"));
        when(_server.multiGet(coordinates, ReadConsistency.STRONG)).thenReturn(expected.iterator());

        List<Map<String, Object>> actual = Lists.newArrayList(
                sorClient(APIKEY_TABLE).multiGet(coordinates));

        assertEquals(actual, expected);
        verify(_server).multiGet(coordinates, ReadConsistency.STRONG);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testMultiGetWithConsistency() {
        List<Map<String, Object>> expected = ImmutableList.<Map<String, Object>>of(
                ImmutableMap.<String, Object>of(Intrinsic.TABLE, "testtable1", Intrinsic.ID, "key1", "count", 1234),
                ImmutableMap.<String, Object>of(Intrinsic.TABLE, "testtable2", Intrinsic.ID, "key&2", "count", 5678));
        List<Coordinate> coordinates = Lists.newArrayList(Coordinate.of("testtable", "key1"),
                Coordinate.of("testtable2", "key&2"));
        when(_server.multiGet(coordinates, ReadConsistency.WEAK)).thenReturn(expected.iterator());

        List<Map<String, Object>> actual = Lists.newArrayList(
                sorClient(APIKEY_TABLE).multiGet(coordinates, ReadConsistency.WEAK));

        assertEquals(actual, expected);
        verify(_server).multiGet(coordinates, ReadConsistency.WEAK);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testUpdate() {
        UUID changeId = TimeUUIDs.newUUID();
        Audit audit = new AuditBuilder().setLocalHost().build();
        List<Update> actualUpdates = Lists.newArrayList();
        //noinspection unchecked
        doUpdateInto(actualUpdates).when(_server).updateAll(any(Iterable.class), anySet());

        sorClient(APIKEY_TABLE).update("table-name", "row-key", changeId, Deltas.literal("hello world"), audit);

        verify(_server).updateAll(any(Iterable.class), anySet());
        assertEquals(actualUpdates,
                Lists.newArrayList(new Update("table-name", "row-key", changeId, Deltas.literal("hello world"), audit,
                        WriteConsistency.STRONG)));
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testUpdateWithConsistency() {
        UUID changeId = TimeUUIDs.newUUID();
        Audit audit = new AuditBuilder().setLocalHost().build();
        List<Update> actualUpdates = Lists.newArrayList();
        //noinspection unchecked
        doUpdateInto(actualUpdates).when(_server).updateAll(any(Iterable.class), anySet());
        sorClient(APIKEY_TABLE).update("table-name", "row-key", changeId, Deltas.literal("hello world"), audit, WriteConsistency.WEAK);

        verify(_server).updateAll(any(Iterable.class), anySet());
        assertEquals(actualUpdates,
                Lists.newArrayList(new Update("table-name", "row-key", changeId, Deltas.literal("hello world"), audit,
                        WriteConsistency.WEAK)));
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testUpdateAll() {
        Audit audit = new AuditBuilder().setLocalHost().build();
        ImmutableList<Update> updates = ImmutableList.of(
                new Update("table-name1", "row-key1", TimeUUIDs.newUUID(), Deltas.literal("hello world"), audit),
                new Update("table-name2", "row-key2", TimeUUIDs.newUUID(), Deltas.literal("hello world"), audit));

        List<Update> actualUpdates = Lists.newArrayList();
        //noinspection unchecked
        doUpdateInto(actualUpdates).when(_server).updateAll(any(Iterable.class), anySet());

        DataStoreStreaming.updateAll(sorClient(APIKEY_TABLE), updates);

        //noinspection unchecked
        verify(_server).updateAll(any(Iterable.class), anySet());
        assertEquals(actualUpdates, updates);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testUpdateAllWithTags() {
        Audit audit = new AuditBuilder().setLocalHost().build();
        ImmutableList<Update> updates = ImmutableList.of(
                new Update("table-name1", "row-key1", TimeUUIDs.newUUID(), Deltas.literal("hello world"), audit),
                new Update("table-name2", "row-key2", TimeUUIDs.newUUID(), Deltas.literal("hello world"), audit));

        List<Update> actualUpdates = Lists.newArrayList();
        Set<String> actualTags = Sets.newHashSet("ignore");
        //noinspection unchecked
        captureUpdatesAndTags(actualUpdates, actualTags).when(_server).updateAll(any(Iterable.class), anySet());

        DataStore client = sorClient(APIKEY_STANDARD_UPDATE);
        client.updateAll(updates, Sets.newHashSet("ignore"));

        assertEquals(actualUpdates, updates);
        assertEquals(actualTags, Sets.newHashSet("ignore"));
        //noinspection unchecked
        verify(_server).updateAll(any(Iterable.class), anySet());
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testUpdateAllWithTagsUsingStreaming() {
        Audit audit = new AuditBuilder().setLocalHost().build();
        ImmutableList<Update> updates = ImmutableList.of(
                new Update("table-name1", "row-key1", TimeUUIDs.newUUID(), Deltas.literal("hello world"), audit),
                new Update("table-name2", "row-key2", TimeUUIDs.newUUID(), Deltas.literal("hello world"), audit));

        List<Update> actualUpdates = Lists.newArrayList();
        Set<String> actualTags = Sets.newHashSet("ignore");
        //noinspection unchecked
        captureUpdatesAndTags(actualUpdates, actualTags).when(_server).updateAll(any(Iterable.class), anySet());

        DataStoreStreaming.updateAll(sorClient(APIKEY_TABLE), updates, Sets.newHashSet("ignore"));

        //noinspection unchecked
        assertEquals(actualUpdates, updates);
        assertEquals(actualTags, Sets.newHashSet("ignore"));
        verify(_server).updateAll(any(Iterable.class), anySet());
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testCompact() {
        sorClient(APIKEY_TABLE).compact("table-name", "row-key", Duration.ofDays(1), ReadConsistency.STRONG, WriteConsistency.STRONG);

        verify(_server).compact("table-name", "row-key", Duration.ofDays(1), ReadConsistency.STRONG, WriteConsistency.STRONG);
        verifyNoMoreInteractions(_server);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testTableExistsException() {
        doThrow(new TableExistsException("table-name"))
                .when(_server)
                .createTable(eq("table-name"), any(TableOptions.class), any(Map.class), any(Audit.class));
        DataCenter dataCenter = mock(DataCenter.class);
        when(_dataCenters.getSelf()).thenReturn(dataCenter);
        when(dataCenter.isSystem()).thenReturn(true);

        TableOptions options = new TableOptionsBuilder().setPlacement("my:placement").build();
        Map<String, String> attributes = ImmutableMap.of("key", "value");
        Audit audit = new AuditBuilder().setLocalHost().build();
        try {
            sorClient(APIKEY_TABLE).createTable("table-name", options, attributes, audit);
            fail();
        } catch (TableExistsException e) {
            assertEquals(e.getTable(), "table-name");
        }
        verify(_server).createTable("table-name", options, attributes, audit);
        //verify(_dataCenters).getSelf();
        verifyNoMoreInteractions(_server, _dataCenters);

    }

    @Test
    public void testUnknownTableException() {
        when(_server.get("table-name", "row-key", ReadConsistency.STRONG))
                .thenThrow(new UnknownTableException("table-name"));
        try {
            sorClient(APIKEY_TABLE).get("table-name", "row-key", ReadConsistency.STRONG);
            fail();
        } catch (UnknownTableException e) {
            assertEquals(e.getTable(), "table-name");
            verify(_server).get("table-name", "row-key", ReadConsistency.STRONG);
            verifyNoMoreInteractions(_server);
        }
    }

    @Test
    public void testUnknownPlacementException() {
        when(_server.get("table-name", "row-key", ReadConsistency.STRONG))
                .thenThrow(new UnknownPlacementException("Table table-name is not available in this data center", "placement-name"));
        try {
            sorClient(APIKEY_TABLE).get("table-name", "row-key", ReadConsistency.STRONG);
            fail();
        } catch (UnknownPlacementException e) {
            assertEquals(e.getPlacement(), "placement-name");
            verify(_server).get("table-name", "row-key", ReadConsistency.STRONG);
            verifyNoMoreInteractions(_server);
        }
    }

    @Test
    public void testGetTablePlacements() {
        List<String> expected = Lists.newArrayList("app_global:default", "ugc_global:ugc", "app_global:sys", "catalog_global:cat");
        when(_server.getTablePlacements()).thenReturn(expected);
        List<String> actual = Lists.newArrayList(sorClient(APIKEY_TABLE).getTablePlacements());

        assertEquals(actual, expected);
        verify(_server).getTablePlacements();
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testUpdateWithEmptyKeyRejected() {
        // Need to iterate over all updates to cause the JSON exception to be thrown
        //noinspection unchecked
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation)
                    throws Throwable {
                //noinspection unchecked
                return Iterables.getLast(((Iterable<Update>) invocation.getArguments()[0]));  // Forces full iteration
            }
        }).when(_server).updateAll(any(Iterable.class), anySet());

        // Because the Update constructor checks for empty keys we need to create it with a non-empty key
        // and then replace it in the JSON map later.

        List<Update> updates = ImmutableList.of(
                new Update("table-name", "to-be-replaced-key", TimeUUIDs.newUUID(),
                        Deltas.literal(ImmutableMap.of("empty", "empty")),
                        new AuditBuilder().setComment("empty value").build())
        );

        List<Map<String, Object>> jsonMaps = JsonHelper.convert(updates, new TypeReference<List<Map<String, Object>>>() {});
        jsonMaps.get(0).put("key", "");
        String json = JsonHelper.asJson(jsonMaps);

        // Again, we can't use the sorClient() because of the Update constructor check, so use the Jersey client directly
        URI uri = UriBuilder.fromUri("/sor/1")
                .segment("_stream")
                .queryParam("batch", 0)
                .queryParam("table", updates.get(0).getTable())
                .queryParam("key", updates.get(0).getKey())
                .queryParam("audit", RisonHelper.asORison(updates.get(0).getAudit()))
                .queryParam("consistency", updates.get(0).getConsistency())
                .build();
        Response response = _resourceTestRule.client().target(uri)
                .request()
                .header(ApiKeyRequest.AUTHENTICATION_HEADER, APIKEY_TABLE)
                .post(Entity.json(json));
        assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
        assertEquals(response.getHeaders().getFirst("X-BV-Exception"), JsonStreamProcessingException.class.getName());

        //noinspection unchecked
        verify(_server).updateAll(any(Iterable.class), anySet());
    }

    @Test
    public void testSystemTablesProtectedFromStandardRole() {
        // Restricted because the name begins with "__"
        when(_server.getTableMetadata("__restricted:name"))
                .thenAnswer(returnMockTable("__restricted:name", "ugc_global:ugc"));
        // Restricted because the placement is a system placement
        when(_server.getTableMetadata("restricted:placement"))
                .thenAnswer(returnMockTable("restricted:placement", "app_global:sys"));
        // Permitted because the name and placement are completely unrestricted
        when(_server.getTableMetadata("permitted:table0"))
                .thenAnswer(returnMockTable("permitted:table0", "ugc_global:ugc"));
        // Permitted even though "sys" appears in the placement prefix
        when(_server.getTableMetadata("permitted:table1"))
                .thenAnswer(returnMockTable("permitted:table1", "sys_global:cat"));
        // Permitted even though "sys" appears in the placement suffix
        when(_server.getTableMetadata("permitted:table2"))
                .thenAnswer(returnMockTable("permitted:table2", "app_global:sysx"));
        // Permitted even though "sys" appears in the placement suffix
        when(_server.getTableMetadata("permitted:table3"))
                .thenAnswer(returnMockTable("permitted:table3", "app_global:xsys"));

        Audit audit = new AuditBuilder().setLocalHost().build();

        for (String tableName : ImmutableList.of("__restricted:name", "restricted:placement")) {
            try {
                sorClient(APIKEY_STANDARD).update(tableName, "key", TimeUUIDs.newUUID(), Deltas.delete(), audit);
                fail("Access not restricted: " + tableName);
            } catch (Exception e) {
                assertTrue(e instanceof UnauthorizedException);
            }
        }

        for (int i = 0; i <= 3; i++) {
            sorClient(APIKEY_STANDARD).update("permitted:table" + i, "key", TimeUUIDs.newUUID(), Deltas.delete(), audit);
        }

        verify(_server, times(6)).getTableMetadata(anyString());
        verify(_server, times(4)).updateAll(any(Iterable.class), anySet());
    }

    private Answer<Table> returnMockTable(final String name, final String placement) {
        return new Answer<Table>() {
            @Override
            public Table answer(InvocationOnMock invocationOnMock) throws Throwable {
                Table table = mock(Table.class);
                when(table.getName()).thenReturn(name);
                when(table.getOptions()).thenReturn(new TableOptionsBuilder().setPlacement(placement).build());
                when(table.getTemplate()).thenReturn(ImmutableMap.<String, Object>of());
                return table;
            }
        };
    }

    @Test (expected = InvalidCredentialException.class)
    public void testClientWithNullApiKey() {
        sorClient(null);
    }

    @Test (expected = InvalidCredentialException.class)
    public void testClientWithEmptyApiKey() {
        sorClient("");
    }
}
