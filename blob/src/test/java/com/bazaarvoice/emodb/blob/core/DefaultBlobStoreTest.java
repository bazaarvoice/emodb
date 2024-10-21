package com.bazaarvoice.emodb.blob.core;

import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.blob.db.MetadataProvider;
import com.bazaarvoice.emodb.blob.db.StorageProvider;
import com.bazaarvoice.emodb.blob.db.StorageSummary;
import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.test.InMemoryTableDAO;
import com.codahale.metrics.MetricRegistry;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


//public class DefaultBlobStoreTest {
//
//    private InMemoryTableDAO tableDao;
//    private StorageProvider storageProvider;
//    private MetadataProvider metadataProvider;
//    private MetricRegistry metricRegistry;
//    private BlobStore blobStore;
//    private static final String TABLE = "table1";
//
//    @BeforeMethod
//    public void setup() {
//        tableDao = new InMemoryTableDAO();
//        storageProvider = mock(StorageProvider.class);
//        metadataProvider = mock(MetadataProvider.class);
//        metricRegistry = mock(MetricRegistry.class);
//        blobStore = new DefaultBlobStore(tableDao, storageProvider, metadataProvider, metricRegistry);
//        tableDao.create(TABLE, new TableOptionsBuilder().setPlacement("placement").build(), new HashMap<String, String>(), new AuditBuilder().setComment("create table").build());
//    }
//
//    @AfterTest
//    public void tearDown() {
//        tableDao.drop(TABLE, new AuditBuilder().setComment("drop table").build());
//    }
//
//    @Test
//    public void testPut() {
//        when(storageProvider.getDefaultChunkSize()).thenReturn(1);
//        String blobId = UUID.randomUUID().toString();
//        try {
//            blobStore.put(TABLE, blobId, () -> new ByteArrayInputStream("b".getBytes()), new HashMap<>());
//        } catch (Exception e) {
//            verify(storageProvider, times(1)).writeChunk(any(Table.class), eq(blobId), anyInt(), any(ByteBuffer.class), anyLong());
//            verifyNoMoreInteractions(storageProvider);
//
//            verify(metadataProvider, times(1)).writeMetadata(any(Table.class), eq(blobId), any(StorageSummary.class));
//            verifyNoMoreInteractions(metadataProvider);
//        }
//    }
//
//    @Test
//    public void testPut_FailedStorageWriteChunk() {
//        when(storageProvider.getDefaultChunkSize()).thenReturn(1);
//        String blobId = UUID.randomUUID().toString();
//        doThrow(new RuntimeException("Cannot write chunk"))
//                .when(storageProvider)
//                .writeChunk(any(Table.class), eq(blobId), anyInt(), any(ByteBuffer.class), anyLong());
//
//        try {
//            blobStore.put(TABLE, blobId, () -> new ByteArrayInputStream("blob-content".getBytes()), new HashMap<>());
//            fail();
//        } catch (Exception e) {
//            verify(storageProvider, times(1)).getCurrentTimestamp(any(Table.class));
//            verify(storageProvider, times(1)).getDefaultChunkSize();
//            verify(storageProvider, times(1)).writeChunk(any(Table.class), eq(blobId), anyInt(), any(ByteBuffer.class), anyLong());
//            verifyNoMoreInteractions(storageProvider);
//
//            verify(metadataProvider, never()).writeMetadata(any(Table.class), eq(blobId), any(StorageSummary.class));
//            verifyNoMoreInteractions(metadataProvider);
//        }
//    }
//
//    @Test
//    public void testPut_FailedWriteMetadata() {
//        when(storageProvider.getDefaultChunkSize()).thenReturn(1);
//        String blobId = UUID.randomUUID().toString();
//        doThrow(new RuntimeException("Cannot write metadata"))
//                .when(metadataProvider)
//                .writeMetadata(any(Table.class), eq(blobId), any(StorageSummary.class));
//        try {
//            blobStore.put(TABLE, blobId, () -> new ByteArrayInputStream("b".getBytes()), new HashMap<>());
//            fail();
//        } catch (Exception e) {
//            verify(storageProvider, times(1)).getCurrentTimestamp(any(Table.class));
//            verify(storageProvider, times(1)).getDefaultChunkSize();
//            verify(storageProvider, times(1)).writeChunk(any(Table.class), eq(blobId), anyInt(), any(ByteBuffer.class), anyLong());
//            verify(storageProvider, times(1)).deleteObject(any(Table.class), eq(blobId));
//            verifyNoMoreInteractions(storageProvider);
//
//            verify(metadataProvider, times(1)).writeMetadata(any(Table.class), eq(blobId), any(StorageSummary.class));
//            verifyNoMoreInteractions(metadataProvider);
//        }
//    }
//
//    @Test
//    public void testPut_FailedDeleteObject() {
//        when(storageProvider.getDefaultChunkSize()).thenReturn(1);
//        String blobId = UUID.randomUUID().toString();
//        doThrow(new RuntimeException("Cannot write metadata"))
//                .when(metadataProvider)
//                .writeMetadata(any(Table.class), eq(blobId), any(StorageSummary.class));
//        doThrow(new RuntimeException("Cannot delete object"))
//                .when(storageProvider)
//                .deleteObject(any(Table.class), eq(blobId));
//        try {
//            blobStore.put(TABLE, blobId, () -> new ByteArrayInputStream("b".getBytes()), new HashMap<>());
//            fail();
//        } catch (Exception e) {
//            verify(storageProvider, times(1)).getCurrentTimestamp(any(Table.class));
//            verify(storageProvider, times(1)).getDefaultChunkSize();
//            verify(storageProvider, times(1)).writeChunk(any(Table.class), eq(blobId), anyInt(), any(ByteBuffer.class), anyLong());
//            verify(storageProvider, times(1)).deleteObject(any(Table.class), eq(blobId));
//            verifyNoMoreInteractions(storageProvider);
//
//            verify(metadataProvider, times(1)).writeMetadata(any(Table.class), eq(blobId), any(StorageSummary.class));
//            verifyNoMoreInteractions(metadataProvider);
//        }
//    }
//
//    @Test
//    public void testDelete_FailedReadMetadata() {
//        String blobId = UUID.randomUUID().toString();
//        doThrow(new RuntimeException("Cannot read metadata"))
//                .when(metadataProvider)
//                .readMetadata(any(Table.class), eq(blobId));
//        try {
//            blobStore.delete("table1", blobId);
//            fail();
//        } catch (Exception e) {
//            verifyNoMoreInteractions(storageProvider);
//
//            verify(metadataProvider, times(1)).readMetadata(any(Table.class), eq(blobId));
//            verifyNoMoreInteractions(metadataProvider);
//        }
//    }
//
//    @Test
//    public void testDelete_FailedDeleteMetadata() {
//        String blobId = UUID.randomUUID().toString();
//        doThrow(new RuntimeException("Cannot delete metadata"))
//                .when(metadataProvider)
//                .deleteMetadata(any(Table.class), eq(blobId));
//        try {
//            blobStore.delete("table1", blobId);
//            fail();
//        } catch (Exception e) {
//            verifyNoMoreInteractions(storageProvider);
//
//            verify(metadataProvider, times(1)).readMetadata(any(Table.class), eq(blobId));
//            verifyNoMoreInteractions(metadataProvider);
//        }
//    }
//
//    @Test
//    public void testDelete_FailedDeleteObject() {
//        doThrow(new RuntimeException("Cannot delete object"))
//                .when(storageProvider)
//                .deleteObject(any(Table.class), anyString());
//        String blobId = UUID.randomUUID().toString();
//        try {
//            blobStore.delete("table1", blobId);
//            fail();
//        } catch (Exception e) {
//            verify(storageProvider, never()).writeChunk(any(Table.class), eq(blobId), anyInt(), any(ByteBuffer.class), anyLong());
//            verifyNoMoreInteractions(storageProvider);
//
//            verify(metadataProvider, times(1)).readMetadata(any(Table.class), eq(blobId));
//            verifyNoMoreInteractions(metadataProvider);
//        }
//    }
//
//    @Test
//    public void testPurgeTableUnsafe() {
//        String blobId1 = UUID.randomUUID().toString();
//        String blobId2 = UUID.randomUUID().toString();
//
//        Map<String, StorageSummary> map = new HashMap<String, StorageSummary>() {{
//            put(blobId1, new StorageSummary(1, 1, 1, "md5_1", "sha1_1", new HashMap<>(), 1));
//            put(blobId2, new StorageSummary(2, 1, 2, "md5_2", "sha1_2", new HashMap<>(), 2));
//        }};
//        when(metadataProvider.scanMetadata(any(Table.class), isNull(), any(LimitCounter.class))).thenReturn(map.entrySet().iterator());
//        blobStore.purgeTableUnsafe(TABLE, new AuditBuilder().setComment("purge").build());
//
//        verify(metadataProvider, times(1)).scanMetadata(any(Table.class), isNull(), any(LimitCounter.class));
//        verify(metadataProvider, times(1)).deleteMetadata(any(Table.class), eq(blobId1));
//        verify(metadataProvider, times(1)).deleteMetadata(any(Table.class), eq(blobId2));
//        verifyNoMoreInteractions(metadataProvider);
//
//        verify(storageProvider, times(1)).deleteObject(any(Table.class), eq(blobId1));
//        verify(storageProvider, times(1)).deleteObject(any(Table.class), eq(blobId2));
//        verifyNoMoreInteractions(storageProvider);
//    }
//
//    @Test
//    public void testPurgeTableUnsafe_EmptyTable() {
//        when(metadataProvider.scanMetadata(any(Table.class), isNull(), any(LimitCounter.class))).thenReturn(new HashMap<String, StorageSummary>().entrySet().iterator());
//        blobStore.purgeTableUnsafe(TABLE, new AuditBuilder().setComment("purge").build());
//        verifyNoMoreInteractions(storageProvider);
//
//        verify(metadataProvider, times(1)).scanMetadata(any(Table.class), isNull(), any(LimitCounter.class));
//        verifyNoMoreInteractions(metadataProvider);
//    }
//
//    @Test
//    public void testPurgeTableUnsafe_FailedScanMetadata() {
//        when(metadataProvider.scanMetadata(any(Table.class), isNull(), any(LimitCounter.class))).thenThrow(new RuntimeException("Failed to scan metadata"));
//        try {
//            blobStore.purgeTableUnsafe(TABLE, new AuditBuilder().setComment("purge").build());
//            fail();
//        } catch (Exception e) {
//            verify(metadataProvider, times(1)).scanMetadata(any(Table.class), isNull(), any(LimitCounter.class));
//            verifyNoMoreInteractions(metadataProvider);
//            verifyNoMoreInteractions(metadataProvider);
//        }
//    }
//
//    @Test
//    public void testPurgeTableUnsafe_FailedDelete() {
//        String blobId1 = UUID.randomUUID().toString();
//        String blobId2 = UUID.randomUUID().toString();
//
//        Map<String, StorageSummary> map = new HashMap<String, StorageSummary>() {{
//            put(blobId1, new StorageSummary(1, 1, 1, "md5_1", "sha1_1", new HashMap<>(), 1));
//            put(blobId2, new StorageSummary(2, 1, 2, "md5_2", "sha1_2", new HashMap<>(), 2));
//        }};
//        when(metadataProvider.scanMetadata(any(Table.class), isNull(), any(LimitCounter.class))).thenReturn(map.entrySet().iterator());
//        doThrow(new RuntimeException("Cannot delete metadata"))
//                .when(metadataProvider)
//                .deleteMetadata(any(Table.class), eq(blobId1));
//
//        try {
//            blobStore.purgeTableUnsafe(TABLE, new AuditBuilder().setComment("purge").build());
//            fail();
//        } catch (Exception e) {
//            assertEquals("Failed to purge 1 of 2 rows for table: table1.", e.getMessage());
//            verify(storageProvider, times(1)).deleteObject(any(Table.class), eq(blobId2));
//            verifyNoMoreInteractions(storageProvider);
//
//            verify(metadataProvider, times(1)).scanMetadata(any(Table.class), isNull(), any(LimitCounter.class));
//            verify(metadataProvider, times(1)).deleteMetadata(any(Table.class), eq(blobId1));
//            verify(metadataProvider, times(1)).deleteMetadata(any(Table.class), eq(blobId2));
//            verifyNoMoreInteractions(metadataProvider);
//        }
//    }
//}
