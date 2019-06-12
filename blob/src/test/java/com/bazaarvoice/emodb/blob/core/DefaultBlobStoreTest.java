package com.bazaarvoice.emodb.blob.core;

import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.blob.db.MetadataProvider;
import com.bazaarvoice.emodb.blob.db.StorageProvider;
import com.bazaarvoice.emodb.blob.db.StorageSummary;
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
import java.util.UUID;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;


public class DefaultBlobStoreTest {
    private StorageProvider storageProvider;
    private MetadataProvider metadataProvider;
    private InMemoryTableDAO tableDao = new InMemoryTableDAO();
    private BlobStore blobStore;
    private static final String TABLE = "table1";

    @BeforeMethod
    public void setup() {
        storageProvider = mock(StorageProvider.class);
        metadataProvider = mock(MetadataProvider.class);
        blobStore = new DefaultBlobStore(tableDao, storageProvider, metadataProvider, new MetricRegistry());
        tableDao.create(TABLE, new TableOptionsBuilder().setPlacement("placement").build(), new HashMap<String, String>(), new AuditBuilder().setComment("create table").build());
    }

    @AfterTest
    public void tearDown() throws Exception {
        tableDao.drop(TABLE, new AuditBuilder().setComment("drop table").build());
    }

    @Test
    public void testPut() {
        when(storageProvider.getDefaultChunkSize()).thenReturn(1);
        String blobId = UUID.randomUUID().toString();
        try {
            blobStore.put(TABLE, blobId, () -> new ByteArrayInputStream("b".getBytes()), new HashMap<>());
        } catch (Exception e) {
            verify(storageProvider, times(1)).writeChunk(any(Table.class), eq(blobId), anyInt(), any(ByteBuffer.class), anyLong());
            verifyNoMoreInteractions(storageProvider);

            verify(metadataProvider, times(1)).writeMetadata(any(Table.class), eq(blobId), any(StorageSummary.class));
            verifyNoMoreInteractions(metadataProvider);
        }
    }

    @Test
    public void testPut_FailedStorageWriteChunk() {
        when(storageProvider.getDefaultChunkSize()).thenReturn(1);
        String blobId = UUID.randomUUID().toString();
        doThrow(new RuntimeException("Cannot write chunk"))
                .when(storageProvider)
                .writeChunk(any(Table.class), eq(blobId), anyInt(), any(ByteBuffer.class), anyLong());

        try {
            blobStore.put(TABLE, blobId, () -> new ByteArrayInputStream("blob-content".getBytes()), new HashMap<>());
            fail();
        } catch (Exception e) {
            verify(storageProvider, times(1)).getCurrentTimestamp(any(Table.class));
            verify(storageProvider, times(1)).getDefaultChunkSize();
            verify(storageProvider, times(1)).writeChunk(any(Table.class), eq(blobId), anyInt(), any(ByteBuffer.class), anyLong());
            verifyNoMoreInteractions(storageProvider);

            verify(metadataProvider, never()).writeMetadata(any(Table.class), eq(blobId), any(StorageSummary.class));
            verifyNoMoreInteractions(metadataProvider);
        }
    }

    @Test
    public void testPut_FailedWriteMetadata() {
        when(storageProvider.getDefaultChunkSize()).thenReturn(1);
        String blobId = UUID.randomUUID().toString();
        doThrow(new RuntimeException("Cannot write metadata"))
                .when(metadataProvider)
                .writeMetadata(any(Table.class), eq(blobId), any(StorageSummary.class));
        try {
            blobStore.put(TABLE, blobId, () -> new ByteArrayInputStream("b".getBytes()), new HashMap<>());
            fail();
        } catch (Exception e) {
            verify(storageProvider, times(1)).getCurrentTimestamp(any(Table.class));
            verify(storageProvider, times(1)).getDefaultChunkSize();
            verify(storageProvider, times(1)).writeChunk(any(Table.class), eq(blobId), anyInt(), any(ByteBuffer.class), anyLong());
            verify(storageProvider, times(1)).deleteObject(any(Table.class), eq(blobId));
            verifyNoMoreInteractions(storageProvider);

            verify(metadataProvider, times(1)).writeMetadata(any(Table.class), eq(blobId), any(StorageSummary.class));
            verifyNoMoreInteractions(metadataProvider);
        }
    }

    @Test
    public void testPut_FailedDeleteObject() {
        when(storageProvider.getDefaultChunkSize()).thenReturn(1);
        String blobId = UUID.randomUUID().toString();
        doThrow(new RuntimeException("Cannot write metadata"))
                .when(metadataProvider)
                .writeMetadata(any(Table.class), eq(blobId), any(StorageSummary.class));
        doThrow(new RuntimeException("Cannot delete object"))
                .when(storageProvider)
                .deleteObject(any(Table.class), eq(blobId));
        try {
            blobStore.put(TABLE, blobId, () -> new ByteArrayInputStream("b".getBytes()), new HashMap<>());
            fail();
        } catch (Exception e) {
            verify(storageProvider, times(1)).getCurrentTimestamp(any(Table.class));
            verify(storageProvider, times(1)).getDefaultChunkSize();
            verify(storageProvider, times(1)).writeChunk(any(Table.class), eq(blobId), anyInt(), any(ByteBuffer.class), anyLong());
            verify(storageProvider, times(1)).deleteObject(any(Table.class), eq(blobId));
            verifyNoMoreInteractions(storageProvider);

            verify(metadataProvider, times(1)).writeMetadata(any(Table.class), eq(blobId), any(StorageSummary.class));
            verifyNoMoreInteractions(metadataProvider);
        }
    }

    @Test
    public void testDelete_FailedReadMetadata() {
        String blobId = UUID.randomUUID().toString();
        doThrow(new RuntimeException("Cannot read metadata"))
                .when(metadataProvider)
                .readMetadata(any(Table.class), eq(blobId));
        try {
            blobStore.delete("table1", blobId);
            fail();
        } catch (Exception e) {
            verifyNoMoreInteractions(storageProvider);

            verify(metadataProvider, times(1)).readMetadata(any(Table.class), eq(blobId));
            verifyNoMoreInteractions(metadataProvider);
        }
    }

    @Test
    public void testDelete_FailedDeleteObject() {
        doThrow(new RuntimeException("Cannot delete object"))
                .when(storageProvider)
                .deleteObject(any(Table.class), anyString());
        String blobId = UUID.randomUUID().toString();
        try {
            blobStore.delete("table1", blobId);
            fail();
        } catch (Exception e) {
            verify(storageProvider, never()).writeChunk(any(Table.class), eq(blobId), anyInt(), any(ByteBuffer.class), anyLong());
            verifyNoMoreInteractions(storageProvider);

            verify(metadataProvider, times(1)).readMetadata(any(Table.class), eq(blobId));
            verifyNoMoreInteractions(metadataProvider);
        }
    }

//TODO
/*    @Test
    public void testPurgeTableUnsafe() {
        blobStore.purgeTableUnsafe(TABLE, new AuditBuilder().setComment("purge").build());
    }

    @Test
    public void testPurgeTableUnsafe_FailedScanMetadata() {
        blobStore.purgeTableUnsafe(TABLE, new AuditBuilder().setComment("purge").build());
    }

    @Test
    public void testPurgeTableUnsafe_FailedDeleteEntry() {
        blobStore.purgeTableUnsafe(TABLE, new AuditBuilder().setComment("purge").build());
    }*/
}