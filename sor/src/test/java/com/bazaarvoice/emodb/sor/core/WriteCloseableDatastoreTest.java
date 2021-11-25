package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.common.api.ServiceUnavailableException;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.Update;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.table.db.TableBackingStore;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


public class WriteCloseableDatastoreTest {

    @Test
    public void testShutdown() throws InterruptedException, ExecutionException {
        Iterable<Update> updateIterable = Iterables.cycle(
                new Update("table-name", "key-name", TimeUUIDs.newUUID(),
                        Deltas.literal(ImmutableMap.of("empty", "empty")),
                        new AuditBuilder().setComment("empty value").build())
        );


        DataStore dataStore = mock(DataStore.class);
        Semaphore iteratorLock = new Semaphore(1);
        Semaphore closeWritesLock = new Semaphore(1);

        doAnswer(invocation -> {
            Iterable<Update> updates = (Iterable<Update>) invocation.getArguments()[0];
            int count = 0;
            for (Update update : updates) {
                count++;
                if (count == 5) {
                    iteratorLock.release();
                    closeWritesLock.acquireUninterruptibly();
                }
            }
            // Check to make sure that writes were closed and the iterator cut off when it was supposed to
            assertEquals(count, 5);

            return null;
        }).when(dataStore).updateAll(any(), any());

        WriteCloseableDataStore writeCloseableDataStore = new WriteCloseableDataStore(dataStore,
                mock(TableBackingStore.class), new MetricRegistry()) {
            @Override
            protected void postWritesClosed() {
                closeWritesLock.release();
            }
        };

        iteratorLock.acquireUninterruptibly();
        closeWritesLock.acquireUninterruptibly();

        Future updateAllFuture = Executors.newSingleThreadExecutor().submit(() -> {
            try {
                writeCloseableDataStore.updateAll(updateIterable);
                fail("Write succeeded when it should have failed due to shutdown. Should have thrown a" +
                        "ServiceUnavailablException.");
            } catch (ServiceUnavailableException e) {
                //expected
            }
        });

        iteratorLock.acquireUninterruptibly();
        Future writeCloserFuture = Executors.newSingleThreadExecutor().submit(writeCloseableDataStore::closeWrites);

        updateAllFuture.get();
        writeCloserFuture.get();

        verify(dataStore).updateAll(any(), any());

    }
}
