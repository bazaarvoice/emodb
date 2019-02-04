package com.bazaarvoice.emodb.table.generic;

import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.table.db.DroppedTableException;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.tableset.BlockFileTableSet;
import com.bazaarvoice.emodb.table.db.tableset.DistributedTableSerializer;
import com.bazaarvoice.emodb.table.db.tableset.TableSerializer;
import com.bazaarvoice.emodb.table.db.test.InMemoryTable;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

public class BlockFileTableSetTest {

    private class TestTableSerializer implements TableSerializer {
        private final Map<Long, InMemoryTable> _tableMap;

        private TestTableSerializer(Map<Long, InMemoryTable> tableMap) {
            _tableMap = tableMap;
        }

        @Override
        public Set<Long> loadAndSerialize(long uuid, OutputStream out)
                throws IOException, UnknownTableException, DroppedTableException {
            try {
                if (uuid == -1) {
                    throw new DroppedTableException("Table dropped", "-1");
                } else if (uuid == -2) {
                    throw new UnknownTableException("Table unknown", "-2");
                }

                InMemoryTable table = _tableMap.get(uuid);
                if (table == null) {
                    throw new UnknownTableException();
                }
                // Serialize the core parts of the table
                ObjectOutputStream objectOut = new ObjectOutputStream(out);
                objectOut.writeObject(table.getName());
                objectOut.writeObject(table.getOptions().getPlacement());
                objectOut.writeObject(table.getAttributes());
                objectOut.flush();

                return ImmutableSet.of(uuid);
            } catch (Exception e) {
                Throwables.propagateIfInstanceOf(e, UnknownTableException.class);
                throw Throwables.propagate(e);
            }        }

        @Override
        public Table deserialize(InputStream in)
                throws IOException {
            try {
                ObjectInputStream objectIn = new ObjectInputStream(in);
                String name = (String) objectIn.readObject();
                String placement = (String) objectIn.readObject();
                //noinspection unchecked
                Map<String, Object> attributes = (Map<String, Object>) objectIn.readObject();
                TableOptions options = new TableOptionsBuilder().setPlacement(placement).build();

                return new InMemoryTable(name, options, attributes);
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }

    @Test
    public void testFileCache() throws Exception {
        Map<Long, InMemoryTable> tableMap = Maps.newHashMap();
        try (BlockFileTableSet snapshot = createTestTableSet(tableMap)) {
            // Read all tables
            for (int uuid=0; uuid < 10000; uuid++) {
                Table table = snapshot.getByUuid(uuid);
                validateTable(uuid, table);
            }

            // Delete the original map, so all values must now be read from the cache
            tableMap.clear();

            // Read tables in random order
            long uuid = 2;
            for (int i=0; i < 1000; i++) {
                uuid = (uuid + 117) % 10000;
                Table table = snapshot.getByUuid(uuid);
                validateTable(uuid, table);
            }
        }
    }

    @Test
    public void testConcurrency() throws Exception {
        Map<Long, InMemoryTable> tableMap = Maps.newHashMap();
        try (BlockFileTableSet snapshot = createTestTableSet(tableMap)) {

            List<Thread> threads = Lists.newArrayListWithCapacity(21);

            for (int t=0; t < 10; t++) {
                final int index = t * 1000;

                // From the bottom
                threads.add(new Thread(new Runnable() {
                    @Override
                    public void run() {
                        for (int uuid=index; uuid < index+1000; uuid++) {
                            snapshot.getByUuid(uuid);
                        }
                    }
                }));

                // From the top
                threads.add(new Thread(new Runnable() {
                    @Override
                    public void run() {
                        for (int uuid=9000-index; uuid < 10000-index; uuid++) {
                            snapshot.getByUuid(uuid);
                        }
                    }
                }));
            }

            // Add one final thread that looks for nothing but unknowns
            threads.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int uuid=10000; uuid < 11000; uuid++) {
                        try {
                            snapshot.getByUuid(uuid);
                            fail("Table should be unknown: " + uuid);
                        } catch (UnknownTableException e) {
                            // ok
                        }
                    }
                }
            }));

            for (Thread thread : threads) {
                thread.start();
            }
            for (Thread thread : threads) {
                thread.join();
            }

            tableMap.clear();

            for (int uuid=0; uuid< 10000; uuid++) {
                Table table = snapshot.getByUuid(uuid);
                validateTable(uuid, table);
            }
        }
    }

    @Test
    public void testUnknownAndDroppedTable() throws Exception {
       try (BlockFileTableSet snapshot = new BlockFileTableSet(new TestTableSerializer(ImmutableMap.<Long, InMemoryTable>of()))) {
           // Call twice so it will be cached the second time
           for (int i=0; i < 2; i++) {
               try {
                   snapshot.getByUuid(-1);
                   fail("dropped table not thrown");
               } catch (DroppedTableException e) {
                   assertEquals(e.getMessage(), "Table dropped");
                   assertEquals(e.getPriorTable(), "-1");
               }

               try {
                   snapshot.getByUuid(-2);
                   fail("unknown table not thrown");
               } catch (UnknownTableException e) {
                   assertEquals(e.getMessage(), "Table unknown");
                   assertEquals(e.getTable(), "-2");
               }
           }
       }
    }

    @Test
    public void testDistributed() throws Exception {
        System.setProperty("zookeeper.admin.enableServer", "false");
        try (
                TestingServer zookeeper = new TestingServer();
                CuratorFramework curator0 = CuratorFrameworkFactory.newClient(zookeeper.getConnectString(),
                        new BoundedExponentialBackoffRetry(100, 1000, 5));
                CuratorFramework curator1 = CuratorFrameworkFactory.newClient(zookeeper.getConnectString(),
                        new BoundedExponentialBackoffRetry(100, 1000, 5))
        ) {
            List<Map<Long, InMemoryTable>> maps = Lists.newArrayListWithCapacity(2);
            List<TestTableSerializer> serializers = Lists.newArrayListWithCapacity(2);
            List<CuratorFramework> curators = Lists.newArrayListWithCapacity(2);

            for (int i=0; i < 2; i++) {
                Map<Long, InMemoryTable> map = Maps.newHashMap();
                map.put((long) i, createTestTableForUuid(i));
                TestTableSerializer serializer = new TestTableSerializer(map);

                CuratorFramework curator = i == 0 ? curator0 : curator1;
                curator.start();

                curator = curator.usingNamespace("path/to/cluster");

                maps.add(map);
                serializers.add(serializer);
                curators.add(curator);
            }

            String tablePath = "/table/path";

            try (
                    DistributedTableSerializer distributed0 = new DistributedTableSerializer(serializers.get(0), curators.get(0), tablePath);
                    DistributedTableSerializer distributed1 = new DistributedTableSerializer(serializers.get(1), curators.get(1), tablePath);
                    BlockFileTableSet tableSet0 = new BlockFileTableSet(distributed0);
                    BlockFileTableSet tableSet1 = new BlockFileTableSet(distributed1)
            ) {
                // Load the table from distributed0 (for this unit test it only exists there)
                Table table = tableSet0.getByUuid(0);
                validateTable(0, table);

                // Now load the table from distributed1 (it doesn't exist there but it does exist in the ZooKeeper node)
                table = tableSet1.getByUuid(0);
                validateTable(0, table);

                // Verify the cached table from distributed0
                table = tableSet0.getByUuid(0);
                validateTable(0, table);

                // Same as above in reverse
                table = tableSet1.getByUuid(1);
                validateTable(1, table);
                table = tableSet0.getByUuid(1);
                validateTable(1, table);
                table = tableSet1.getByUuid(1);
                validateTable(1, table);

                // Validate a non-existent table is cached
                try {
                    tableSet0.getByUuid(2);
                    fail("Unknown table exception not thrown");
                } catch (UnknownTableException e) {
                    // ok
                }

                // Add the table to tableSet1 so that it would exist if the result weren't cached
                maps.get(1).put(2L, createTestTableForUuid(2));

                try {
                    tableSet1.getByUuid(2);
                    fail("Unknown table exception not thrown");
                } catch (UnknownTableException e) {
                    // ok
                }
            }
        }
    }

    private InMemoryTable createTestTableForUuid(long uuid) {
        String name = "table_" + uuid;
        TableOptions options = new TableOptionsBuilder().setPlacement("placement" + (uuid % 3)).build();
        Map<String, Object> attributes = ImmutableMap.<String, Object>of(
                "type", "review", "client", "client" + uuid);
        return new InMemoryTable(name, options, attributes);
    }

    private BlockFileTableSet createTestTableSet(Map<Long, InMemoryTable> tableMap) {
        for (long uuid=0; uuid < 10000; uuid++) {
            tableMap.put(uuid, createTestTableForUuid(uuid));
        }

        return new BlockFileTableSet(new TestTableSerializer(tableMap), 50000, 20);
    }

    private void validateTable(long uuid, Table table) {
        assertNotNull(table);
        assertEquals(table.getName(), "table_" + uuid);
        assertEquals(table.getOptions().getPlacement(), "placement" + uuid % 3);
        assertEquals(table.getAttributes(), ImmutableMap.<String, Object>of("type", "review", "client", "client" + uuid));
    }
}
