package test.integration.blob;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.identity.IdentityState;
import com.bazaarvoice.emodb.auth.identity.InMemoryAuthIdentityManager;
import com.bazaarvoice.emodb.auth.permissions.InMemoryPermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.bazaarvoice.emodb.blob.api.Blob;
import com.bazaarvoice.emodb.blob.api.BlobMetadata;
import com.bazaarvoice.emodb.blob.api.BlobNotFoundException;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.blob.api.DefaultBlob;
import com.bazaarvoice.emodb.blob.api.DefaultBlobMetadata;
import com.bazaarvoice.emodb.blob.api.DefaultTable;
import com.bazaarvoice.emodb.blob.api.Range;
import com.bazaarvoice.emodb.blob.api.RangeNotSatisfiableException;
import com.bazaarvoice.emodb.blob.api.RangeSpecifications;
import com.bazaarvoice.emodb.blob.api.StreamSupplier;
import com.bazaarvoice.emodb.blob.api.Table;
import com.bazaarvoice.emodb.blob.client.BlobStoreAuthenticator;
import com.bazaarvoice.emodb.blob.client.BlobStoreClient;
import com.bazaarvoice.emodb.blob.client.BlobStoreStreaming;
import com.bazaarvoice.emodb.common.api.UnauthorizedException;
import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.FacadeOptions;
import com.bazaarvoice.emodb.sor.api.TableAvailability;
import com.bazaarvoice.emodb.sor.api.TableExistsException;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.UnknownPlacementException;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.test.ResourceTest;
import com.bazaarvoice.emodb.web.auth.EmoPermissionResolver;
import com.bazaarvoice.emodb.web.resources.blob.BlobStoreResource1;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.InputSupplier;
import com.sun.jersey.api.client.ClientResponse;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest.AUTHENTICATION_HEADER;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

/**
 * Tests the api calls made via the Jersey HTTP client {@link BlobStoreClient} are
 * interpreted correctly by the Jersey HTTP resource {@link BlobStoreResource1}.
 */
public class BlobStoreJerseyTest extends ResourceTest {
    private static final String APIKEY_BLOB = "blob-key";
    private static final String APIKEY_UNAUTHORIZED = "unauthorized-key";
    private static final String APIKEY_BLOB_A = "a-blob-key";
    private static final String APIKEY_BLOB_B = "b-blob-key";

    private BlobStore _server = mock(BlobStore.class);
    private DataCenters _dataCenters = mock(DataCenters.class);
    private ScheduledExecutorService _connectionManagementService = mock(ScheduledExecutorService.class);
    private Set<String> _approvedContentTypes = ImmutableSet.of("application/json");

    @Rule
    public ResourceTestRule _resourceTestRule = setupResourceTestRule();

    private ResourceTestRule setupResourceTestRule() {
        final InMemoryAuthIdentityManager<ApiKey> authIdentityManager = new InMemoryAuthIdentityManager<>(ApiKey.class);
        authIdentityManager.updateIdentity(new ApiKey(APIKEY_BLOB, "id0", IdentityState.ACTIVE, ImmutableSet.of("blob-role")));
        authIdentityManager.updateIdentity(new ApiKey(APIKEY_UNAUTHORIZED, "id1", IdentityState.ACTIVE, ImmutableSet.of("unauthorized-role")));
        authIdentityManager.updateIdentity(new ApiKey(APIKEY_BLOB_A, "id2", IdentityState.ACTIVE, ImmutableSet.of("blob-role-a")));
        authIdentityManager.updateIdentity(new ApiKey(APIKEY_BLOB_B, "id3", IdentityState.ACTIVE, ImmutableSet.of("blob-role-b")));

        final EmoPermissionResolver permissionResolver = new EmoPermissionResolver(mock(DataStore.class), _server);
        final InMemoryPermissionManager permissionManager = new InMemoryPermissionManager(permissionResolver);
        permissionManager.updateForRole("blob-role", new PermissionUpdateRequest().permit("blob|*|*"));
        permissionManager.updateForRole("blob-role-a", new PermissionUpdateRequest().permit("blob|read|a*"));
        permissionManager.updateForRole("blob-role-b", new PermissionUpdateRequest().permit("blob|read|b*"));

        return setupResourceTestRule(
            Collections.<Object>singletonList(new BlobStoreResource1(_server, _dataCenters, _approvedContentTypes)),
            authIdentityManager,
            permissionManager);
    }

    @After
    public void tearDownMocksAndClearState() {
        verifyNoMoreInteractions(_server, _dataCenters);
        reset(_server, _dataCenters, _connectionManagementService);
    }

    private BlobStore blobClient() {
        return blobClient(APIKEY_BLOB);
    }

    private BlobStore blobClient(String apiKey) {
        return BlobStoreAuthenticator.proxied(new BlobStoreClient(URI.create("/blob/1"),
            new JerseyEmoClient(_resourceTestRule.client()), _connectionManagementService))
            .usingCredentials(apiKey);
    }

    @Test public void testListTablesRestricted() {
        TableOptions options = new TableOptionsBuilder().setPlacement("my:placement").build();
        TableAvailability availability = new TableAvailability("my:placement", false);
        final DefaultTable aTable = new DefaultTable("a-table-1", options, ImmutableMap.of("key", "value1"), availability);
        final DefaultTable bTable = new DefaultTable("b-table-2", options, ImmutableMap.of("key", "value2"), availability);
        final List<Table> expected = ImmutableList.<Table>of(aTable, bTable);

        when(_server.listTables(null, Long.MAX_VALUE)).thenAnswer(new Answer<Iterator<Table>>() {
            @Override public Iterator<Table> answer(final InvocationOnMock invocation) throws Throwable {
                return expected.iterator();
            }
        });

        {
            List<Table> actual = Lists.newArrayList(BlobStoreStreaming.listTables(blobClient(APIKEY_BLOB_A)));
            assertEquals(actual, ImmutableList.of(aTable));
        }
        {
            List<Table> actual = Lists.newArrayList(BlobStoreStreaming.listTables(blobClient(APIKEY_BLOB_B)));
            assertEquals(actual, ImmutableList.of(bTable));
        }
        verify(_server, times(2)).listTables(null, Long.MAX_VALUE);
    }

    @Test public void getTableAttributesRestricted() {
        final ImmutableMap<String, String> expected = ImmutableMap.of("asdf", "asdf");
        when(_server.getTableAttributes("a-table")).thenReturn(expected);

        {
            final Map<String, String> tableAttributes = blobClient(APIKEY_BLOB_A).getTableAttributes("a-table");
            assertEquals(expected, tableAttributes);
        }

        {
            try {
                blobClient(APIKEY_BLOB_B).getTableAttributes("a-table");
                fail();
            } catch (Exception e) {
                assertTrue(e instanceof UnauthorizedException);
            }
        }

        verify(_server).getTableAttributes("a-table");
    }

    @Test public void getTableOptionsRestricted() {
        final TableOptions expected = new TableOptionsBuilder().setFacades(ImmutableList.<FacadeOptions>of()).setPlacement("asdf").build();
        when(_server.getTableOptions("a-table")).thenReturn(expected);

        {
            final TableOptions options = blobClient(APIKEY_BLOB_A).getTableOptions("a-table");
            assertEquals(expected, options);
        }

        {
            try {
                blobClient(APIKEY_BLOB_B).getTableOptions("a-table");
                fail();
            } catch (Exception e) {
                assertTrue(e instanceof UnauthorizedException);
            }
        }

        verify(_server).getTableOptions("a-table");
    }

    @Test public void getTableSizeRestricted() {
        when(_server.getTableApproximateSize("a-table")).thenReturn(1234L);

        {
            final long size = blobClient(APIKEY_BLOB_A).getTableApproximateSize("a-table");
            assertEquals(1234L, size);
        }

        {
            try {
                blobClient(APIKEY_BLOB_B).getTableApproximateSize("a-table");
            } catch (Exception e) {
                assertTrue(e instanceof UnauthorizedException);
            }
        }

        verify(_server).getTableApproximateSize("a-table");
    }

    @Test public void getTableMetadataRestricted() {
        final TableOptions options = new TableOptionsBuilder().setPlacement("my:placement").build();
        final TableAvailability availability = new TableAvailability("my:placement", false);
        final DefaultTable expected = new DefaultTable("a-table", options, ImmutableMap.of("key", "value1"), availability);
        when(_server.getTableMetadata("a-table")).thenReturn(expected);

        {
            final Table table = blobClient(APIKEY_BLOB_A).getTableMetadata("a-table");
            assertEquals(expected, table);
        }

        {
            try {
                blobClient(APIKEY_BLOB_B).getTableMetadata("a-table");
                fail();
            } catch (Exception e) {
                assertTrue(e instanceof UnauthorizedException);
            }
        }

        verify(_server).getTableMetadata("a-table");
    }

    @Test public void getMetadataRestricted() {
        final DefaultBlobMetadata expected = new DefaultBlobMetadata("asdf", new Date(), 1234L, "deadbeef", "qwerpoiu", ImmutableMap.of("key", "value"));
        when(_server.getMetadata("a-table", "asdf")).thenReturn(expected);

        {
            final BlobMetadata metadata = blobClient(APIKEY_BLOB_A).getMetadata("a-table", "asdf");
            assertMetadataEquals(expected, metadata);
        }

        {
            try {
                blobClient(APIKEY_BLOB_B).getMetadata("a-table", "asdf");
                fail();
            } catch (Exception e) {
                assertTrue(e instanceof UnauthorizedException);
            }
        }

        verify(_server).getMetadata("a-table", "asdf");
    }

    @Test public void scanTableRestricted() {
        final List<BlobMetadata> expected = ImmutableList.<BlobMetadata>of(new DefaultBlobMetadata("asdf", new Date(), 1234L, "deadbeef", "qwerpoiu", ImmutableMap.of("key", "value")));
        when(_server.scanMetadata("a-table", null, 10L)).thenAnswer(new Answer<Iterator<BlobMetadata>>() {
            @Override public Iterator<BlobMetadata> answer(final InvocationOnMock invocation) throws Throwable {
                return expected.iterator();
            }
        });

        {
            final List<BlobMetadata> metadata = ImmutableList.copyOf(blobClient(APIKEY_BLOB_A).scanMetadata("a-table", null, 10L));
            assertEquals(1, metadata.size());
            assertMetadataEquals(expected.get(0), metadata.get(0));
        }

        {
            try {
                blobClient(APIKEY_BLOB_B).scanMetadata("a-table", null, 10L);
                fail();
            } catch (Exception e) {
                assertTrue(e instanceof UnauthorizedException);
            }
        }

        verify(_server).scanMetadata("a-table", null, 10L);
    }

    @Test public void getRestricted() {
        final byte[] expected = "blob-content".getBytes();
        Map<String, String> attributes = ImmutableMap.of("key", "value");
        BlobMetadata expectedMd = new DefaultBlobMetadata("blob-id", new Date(), expected.length, "33d5", "54a1", attributes);
        Range expectedRange = new Range(0, expected.length);
        when(_server.get("a-table", "blob-id", null))
            .thenReturn(new DefaultBlob(expectedMd, expectedRange, new StreamSupplier() {
                @Override
                public void writeTo(OutputStream out) throws IOException {
                    out.write(expected);
                }
            }));

        {
            final Blob actual = blobClient(APIKEY_BLOB_A).get("a-table", "blob-id");

            assertMetadataEquals(actual, expectedMd);
            assertEquals(actual.getByteRange(), expectedRange);

            ByteArrayOutputStream actualBytes = new ByteArrayOutputStream();
            try { actual.writeTo(actualBytes); } catch (IOException e) { throw new RuntimeException(e); }
            assertArrayEquals(actualBytes.toByteArray(), expected);
        }

        {
            try {
                blobClient(APIKEY_BLOB_B).get("a-table", "blob-id");
                fail();
            } catch (Exception e) {
                assertTrue(e instanceof UnauthorizedException);
            }
        }

        verify(_server).get("a-table", "blob-id", null);
    }

    @Test
    public void testListTables() {
        TableOptions options = new TableOptionsBuilder().setPlacement("my:placement").build();
        TableAvailability availability = new TableAvailability("my:placement", false);
        List<Table> expected = ImmutableList.<Table>of(
            new DefaultTable("table-1", options, ImmutableMap.of("key", "value1"), availability),
            new DefaultTable("table-2", options, ImmutableMap.of("key", "value2"), availability));
        when(_server.listTables(null, Long.MAX_VALUE)).thenReturn(expected.iterator());

        List<Table> actual = Lists.newArrayList(
            BlobStoreStreaming.listTables(blobClient()));

        assertEquals(expected, actual);
        verify(_server).listTables(null, Long.MAX_VALUE);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testListTablesFrom() {
        TableOptions options = new TableOptionsBuilder().setPlacement("my:placement").build();
        TableAvailability availability = new TableAvailability("my:placement", false);
        List<Table> expected = ImmutableList.<Table>of(
            new DefaultTable("table-1", options, ImmutableMap.of("key", "value1"), availability),
            new DefaultTable("blob-id-2", options, ImmutableMap.of("key", "value2"), availability));
        when(_server.listTables("from-key", 1234L)).thenReturn(expected.iterator());

        List<Table> actual = Lists.newArrayList(
            BlobStoreStreaming.listTables(blobClient(), "from-key", 1234L));

        assertEquals(actual, expected);
        verify(_server).listTables("from-key", 1234L);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testCreateTable() {
        DataCenter dataCenter = mock(DataCenter.class);
        when(_dataCenters.getSelf()).thenReturn(dataCenter);
        when(dataCenter.isSystem()).thenReturn(true);

        TableOptions options = new TableOptionsBuilder().setPlacement("my:placement").build();
        Map<String, String> attributes = ImmutableMap.of("key", "value");
        Audit audit = new AuditBuilder().setLocalHost().build();
        blobClient().createTable("table-name", options, attributes, audit);

        verify(_server).createTable("table-name", options, attributes, audit);
        verify(_dataCenters).getSelf();
        verifyNoMoreInteractions(_server, _dataCenters);
    }

    @Test
    public void testCreateTableUnauthorized() {
        DataCenter dataCenter = mock(DataCenter.class);
        when(_dataCenters.getSelf()).thenReturn(dataCenter);
        when(dataCenter.isSystem()).thenReturn(true);

        TableOptions options = new TableOptionsBuilder().setPlacement("my:placement").build();
        Map<String, String> attributes = ImmutableMap.of("key", "value");
        Audit audit = new AuditBuilder().setLocalHost().build();

        try {
            blobClient(APIKEY_UNAUTHORIZED).createTable("table-name", options, attributes, audit);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof UnauthorizedException);
        }

        verify(_dataCenters).getSelf();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDropTable() {
        Audit audit = new AuditBuilder().setLocalHost().build();
        blobClient().dropTable("table-name", audit);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPurgeTableUnsafe() {
        Audit audit = new AuditBuilder().setLocalHost().build();
        blobClient().purgeTableUnsafe("table-name", audit);
    }

    @Test
    public void testGetTableExists() {
        boolean expected = true;
        when(_server.getTableAttributes("table-name")).thenReturn(ImmutableMap.<String, String>of());

        boolean actual = blobClient().getTableExists("table-name");

        assertEquals(actual, expected);
        verify(_server).getTableAttributes("table-name");
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetTableNotExists() {
        boolean expected = false;
        when(_server.getTableAttributes("table-name")).thenThrow(new UnknownTableException());

        boolean actual = blobClient().getTableExists("table-name");

        assertEquals(actual, expected);
        verify(_server).getTableAttributes("table-name");
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetTableAvailable() {
        boolean expected = true;
        TableAvailability availability = new TableAvailability("my:placement", false);
        TableOptions options = new TableOptionsBuilder().setPlacement("my:placement").build();
        when(_server.getTableMetadata("table-name")).thenReturn(
            new DefaultTable("table-1", options, ImmutableMap.of("key", "value1"), availability)
        );

        boolean actual = blobClient().isTableAvailable("table-name");

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
            new DefaultTable("table-1", options, ImmutableMap.of("key", "value1"), availability)
        );

        boolean actual = blobClient().isTableAvailable("table-name");

        assertEquals(actual, expected);
        verify(_server).getTableMetadata("table-name");
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetTableAttributes() {
        Map<String, String> expected = ImmutableMap.of("key", "value");
        when(_server.getTableAttributes("table-name")).thenReturn(expected);

        Map<String, String> actual = blobClient().getTableAttributes("table-name");

        assertEquals(actual, expected);
        verify(_server).getTableAttributes("table-name");
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetTableOptions() {
        TableOptions expected = new TableOptionsBuilder().setPlacement("my:placement").build();
        when(_server.getTableOptions("table-name")).thenReturn(expected);

        TableOptions actual = blobClient().getTableOptions("table-name");

        assertEquals(actual, expected);
        verify(_server).getTableOptions("table-name");
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetTableApproximateSize() {
        long expected = 1234L;
        when(_server.getTableApproximateSize("table-name")).thenReturn(expected);

        long actual = blobClient().getTableApproximateSize("table-name");

        assertEquals(actual, expected);
        verify(_server).getTableApproximateSize("table-name");
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetMetadata() {
        Map<String, String> attributes = ImmutableMap.of("key", "value");
        BlobMetadata expected = new DefaultBlobMetadata("blob-id", new Date(), 1234, "33d5", "54a1", attributes);
        when(_server.getMetadata("table-name", "blob-id")).thenReturn(expected);

        BlobMetadata actual = blobClient().getMetadata("table-name", "blob-id");

        assertMetadataEquals(expected, actual);
        verify(_server).getMetadata("table-name", "blob-id");
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testScanMetadata() {
        List<BlobMetadata> expected = ImmutableList.<BlobMetadata>of(
            new DefaultBlobMetadata("blob-id-1", new Date(), 55, "33d5", "54a1", ImmutableMap.of("key", "value1")),
            new DefaultBlobMetadata("blob-id-2", new Date(), 66, "33d5", "54a1", ImmutableMap.of("key", "value2")));
        when(_server.scanMetadata("table-name", null, Long.MAX_VALUE)).thenReturn(expected.iterator());

        List<BlobMetadata> actual = Lists.newArrayList(
            BlobStoreStreaming.scanMetadata(blobClient(), "table-name"));

        assertEquals(actual.size(), expected.size());
        for (int i = 0; i < actual.size(); i++) {
            assertMetadataEquals(actual.get(i), expected.get(i));
        }
        verify(_server).scanMetadata("table-name", null, Long.MAX_VALUE);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testScanMetadataFrom() {
        List<BlobMetadata> expected = ImmutableList.<BlobMetadata>of(
            new DefaultBlobMetadata("blob-id-1", new Date(), 55, "33d5", "54a1", ImmutableMap.of("key", "value1")),
            new DefaultBlobMetadata("blob-id-2", new Date(), 66, "33d5", "54a1", ImmutableMap.of("key", "value2")));
        when(_server.scanMetadata("table-name", "from-key", 1234L)).thenReturn(expected.iterator());

        List<BlobMetadata> actual = Lists.newArrayList(
            BlobStoreStreaming.scanMetadata(blobClient(), "table-name", "from-key", 1234L));

        assertEquals(actual.size(), expected.size());
        for (int i = 0; i < actual.size(); i++) {
            assertMetadataEquals(actual.get(i), expected.get(i));
        }
        verify(_server).scanMetadata("table-name", "from-key", 1234L);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGet() throws IOException {
        final byte[] expected = "blob-content".getBytes();
        Map<String, String> attributes = ImmutableMap.of("key", "value");
        BlobMetadata expectedMd = new DefaultBlobMetadata("blob-id", new Date(), expected.length, "33d5", "54a1", attributes);
        Range expectedRange = new Range(0, expected.length);
        when(_server.get("table-name", "blob-id", null))
            .thenReturn(new DefaultBlob(expectedMd, expectedRange, new StreamSupplier() {
                @Override
                public void writeTo(OutputStream out) throws IOException {
                    out.write(expected);
                }
            }));

        Blob actual = blobClient().get("table-name", "blob-id");

        assertMetadataEquals(actual, expectedMd);
        assertEquals(actual.getByteRange(), expectedRange);

        // Read the blob twice to force an automated round-trip on the second request
        for (int i = 0; i < 2; i++) {
            ByteArrayOutputStream actualBytes = new ByteArrayOutputStream();
            actual.writeTo(actualBytes);
            assertArrayEquals(actualBytes.toByteArray(), expected);
        }

        verify(_server, times(2)).get("table-name", "blob-id", null);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetWithOffsetRange() throws IOException {
        final byte[] expected = "blob-content".getBytes();
        Map<String, String> attributes = ImmutableMap.of("key", "value");
        BlobMetadata expectedMd = new DefaultBlobMetadata("blob-id", new Date(), 1234, "33d5", "54a1", attributes);
        Range expectedRange = new Range(2, expected.length);
        when(_server.get("table-name", "blob-id", RangeSpecifications.slice(2, expected.length)))
            .thenReturn(new DefaultBlob(expectedMd, expectedRange, new StreamSupplier() {
                @Override
                public void writeTo(OutputStream out) throws IOException {
                    out.write(expected);
                }
            }));

        Blob actual = blobClient().get("table-name", "blob-id", RangeSpecifications.slice(2, expected.length));

        assertMetadataEquals(actual, expectedMd);
        assertEquals(actual.getByteRange(), expectedRange);

        // Read the blob twice to force an automated round-trip on the second request
        for (int i = 0; i < 2; i++) {
            assertArrayEquals(read(actual), expected);
        }

        verify(_server, times(2)).get("table-name", "blob-id", RangeSpecifications.slice(2, expected.length));
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetWithSuffixRange() throws IOException {
        final byte[] expected = "blob-content".getBytes();
        Map<String, String> attributes = ImmutableMap.of("key", "value");
        BlobMetadata expectedMd = new DefaultBlobMetadata("blob-id", new Date(), 1234, "33d5", "54a1", attributes);
        Range expectedRange = new Range(1234 - expected.length, expected.length);
        when(_server.get("table-name", "blob-id", RangeSpecifications.suffix(expected.length)))
            .thenReturn(new DefaultBlob(expectedMd, expectedRange, new StreamSupplier() {
                @Override
                public void writeTo(OutputStream out) throws IOException {
                    out.write(expected);
                }
            }));

        Blob actual = blobClient().get("table-name", "blob-id", RangeSpecifications.suffix(expected.length));

        assertMetadataEquals(actual, expectedMd);
        assertEquals(actual.getByteRange(), expectedRange);
        assertArrayEquals(read(actual), expected);
        verify(_server).get("table-name", "blob-id", RangeSpecifications.suffix(expected.length));
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetClosure() throws Exception {
        final byte[] expected = "blob-content".getBytes();
        Map<String, String> attributes = ImmutableMap.of("key", "value");
        BlobMetadata expectedMd = new DefaultBlobMetadata("blob-id", new Date(), expected.length, "33d5", "54a1", attributes);
        Range expectedRange = new Range(0, expected.length);
        when(_server.get("table-name", "blob-id", null))
            .thenReturn(new DefaultBlob(expectedMd, expectedRange, new StreamSupplier() {
                @Override
                public void writeTo(OutputStream out) throws IOException {
                    out.write(expected);
                }
            }));

        Blob actual = blobClient().get("table-name", "blob-id");

        // Ensure the runnable which closes the connection was scheduled as expected
        ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        verify(_connectionManagementService).schedule(captor.capture(), eq(2000L), eq(TimeUnit.MILLISECONDS));

        // Run the timeout runnable now
        captor.getValue().run();

        assertMetadataEquals(actual, expectedMd);
        assertEquals(actual.getByteRange(), expectedRange);

        ByteArrayOutputStream actualBytes = new ByteArrayOutputStream();
        actual.writeTo(actualBytes);
        assertArrayEquals(actualBytes.toByteArray(), expected);

        // Even though we only read the blob once it should have performed two back-end calls
        verify(_server, times(2)).get("table-name", "blob-id", null);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testPut() throws IOException {
        InputSupplier<InputStream> in = new InputSupplier<InputStream>() {
            @Override
            public InputStream getInput() throws IOException {
                return new ByteArrayInputStream("blob-content".getBytes());
            }
        };
        Map<String, String> attributes = ImmutableMap.of("key", "value");
        blobClient().put("table-name", "blob-id", in, attributes, Duration.standardDays(30));

        //noinspection unchecked
        verify(_server).put(eq("table-name"), eq("blob-id"), isA(InputSupplier.class), eq(attributes), eq(Duration.standardDays(30)));
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testDelete() {
        blobClient().delete("table-name", "blob-id");

        verify(_server).delete("table-name", "blob-id");
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testBlobNotFound() {
        when(_server.get("table-name", "blob-id", null)).thenThrow(new BlobNotFoundException("blob-id"));
        try {
            blobClient().get("table-name", "blob-id");
            fail();
        } catch (BlobNotFoundException e) {
            assertEquals(e.getBlobId(), "blob-id");
        }
        verify(_server).get("table-name", "blob-id", null);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testBlobMetadataNotFound() {
        when(_server.getMetadata("table-name", "blob-id")).thenThrow(new BlobNotFoundException("blob-id"));
        try {
            blobClient().getMetadata("table-name", "blob-id");
            fail();
        } catch (BlobNotFoundException e) {
            // Expected.
            assertEquals(e.getBlobId(), "blob-id");
        }
        verify(_server).getMetadata("table-name", "blob-id");
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testRangeNotSatisfiable() {
        when(_server.get("table-name", "blob-id", null))
            .thenThrow(new RangeNotSatisfiableException("message", 1, 2));
        try {
            blobClient().get("table-name", "blob-id");
            fail();
        } catch (RangeNotSatisfiableException e) {
            assertEquals(e.getOffset(), 1);
            assertEquals(e.getLength(), 2);
        }
        verify(_server).get("table-name", "blob-id", null);
        verifyNoMoreInteractions(_server);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testTableExistsException() {
        DataCenter dataCenter = mock(DataCenter.class);
        when(_dataCenters.getSelf()).thenReturn(dataCenter);
        when(dataCenter.isSystem()).thenReturn(true);
        doThrow(new TableExistsException("table-name"))
            .when(_server)
            .createTable(eq("table-name"), any(TableOptions.class), any(Map.class), any(Audit.class));

        TableOptions options = new TableOptionsBuilder().setPlacement("my:placement").build();
        Map<String, String> attributes = ImmutableMap.of("key", "value");
        Audit audit = new AuditBuilder().setLocalHost().build();
        try {
            blobClient().createTable("table-name", options, attributes, audit);
            fail();
        } catch (TableExistsException e) {
            assertEquals(e.getTable(), "table-name");
        }
        verify(_server).createTable("table-name", options, attributes, audit);
        verify(_dataCenters).getSelf();
        verifyNoMoreInteractions(_server, _dataCenters);
    }

    @Test
    public void testUnknownTableException() {
        when(_server.get("table-name", "blob-id", null)).thenThrow(new UnknownTableException("table-name"));
        try {
            blobClient().get("table-name", "blob-id");
            fail();
        } catch (UnknownTableException e) {
            assertEquals(e.getTable(), "table-name");
        }
        verify(_server).get("table-name", "blob-id", null);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testGetTablePlacements() {
        List<String> expected = Lists.newArrayList("media_global:ugc");
        when(_server.getTablePlacements()).thenReturn(expected);
        List<String> actual = Lists.newArrayList(blobClient().getTablePlacements());

        assertEquals(actual, expected);
        verify(_server).getTablePlacements();
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testUnknownPlacementException() {
        when(_server.get("table-name", "blob-id", null)).thenThrow(
            new UnknownPlacementException("Table table-name is not available in this data center", "placement-name"));
        try {
            blobClient().get("table-name", "blob-id");
            fail();
        } catch (UnknownPlacementException e) {
            assertEquals(e.getPlacement(), "placement-name");
        }
        verify(_server).get("table-name", "blob-id", null);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testApprovedContentType() throws Exception {
        testContentType("{}".getBytes(Charsets.UTF_8), "application/json", "application/json");
    }

    @Test
    public void testUnapprovedContentType() throws Exception {
        testContentType("<html/>".getBytes(Charsets.UTF_8), "text/html", "application/octet-stream");
    }

    private void testContentType(byte[] content, String metadataContentType, String expectedContentType) throws Exception {
        Map<String, String> attributes = ImmutableMap.of("content-type", metadataContentType);
        BlobMetadata expectedMd = new DefaultBlobMetadata("blob-id", new Date(), content.length, "00", "ff", attributes);
        Range expectedRange = new Range(0, content.length);
        when(_server.get("table-name", "blob-id", null))
                .thenReturn(new DefaultBlob(expectedMd, expectedRange, out -> out.write(content)));

        // The blob store client doesn't interact directly with the Content-Type header.  Therefore this test must bypass
        // and use the underlying client.

        ClientResponse response = _resourceTestRule.client().resource("/blob/1/table-name/blob-id")
                .accept("*")
                .header(AUTHENTICATION_HEADER, APIKEY_BLOB)
                .get(ClientResponse.class);

        assertEquals(response.getStatus(), 200);
        assertEquals(response.getHeaders().getFirst("X-BVA-content-type"), metadataContentType);
        assertEquals(response.getType().toString(), expectedContentType);

        byte[] actual = new byte[content.length];
        InputStream in = response.getEntityInputStream();
        assertEquals(in.read(actual, 0, content.length), content.length);
        assertEquals(in.read(), -1);
        assertArrayEquals(actual, content);

        verify(_server).get("table-name", "blob-id", null);
    }

    private void assertMetadataEquals(BlobMetadata expected, BlobMetadata actual) {
        assertEquals(actual.getId(), expected.getId());
        assertEquals(actual.getLength(), expected.getLength());
        assertEquals(actual.getMD5(), expected.getMD5());
        assertEquals(actual.getSHA1(), expected.getSHA1());
        assertEquals(actual.getAttributes(), expected.getAttributes());
    }

    private byte[] read(Blob actual) throws IOException {
        ByteArrayOutputStream actualBytes = new ByteArrayOutputStream();
        actual.writeTo(actualBytes);
        return actualBytes.toByteArray();
    }
}
