package test.integration.sor;

import com.bazaarvoice.emodb.common.cassandra.cqldriver.AdaptiveResultSet;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.FrameTooLongException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.dropwizard.util.Size;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * This unit test verifies that {@link AdaptiveResultSet} used by {@link com.bazaarvoice.emodb.sor.db.astyanax.CqlDataReaderDAO}
 * works correctly.  Ideally CqlDataReader would be tested directly but variability in underlying data due to table
 * sharding, delta formats, and compaction make this difficult to control.  This unit test therefore only tests
 * AdaptiveResultSet in a controlled environment.  One can then extrapolate that it works in the data reader context
 * as well.
 *
 * This test works by connecting to an external Cassandra cluster.  For now this test is hard-coded to use the cluster
 * created in the web-local package by running <tt>start.sh</tt> or <tt>start-cassandra.sh</tt> or by running integration
 * tests.  It works by creating a keyspace with a single table and then populating that table with large rows.
 * Different types of queries are then made to ensure all work when the maximum frame size is exceeded.
 *
 * Because of the amount of IO this test requires it is a long running test and depending on the test environment it could
 * take over a minute to complete.  For this reason this test is disabled by default.  To run this test you must set the
 * testng parameter "runAdaptiveCqlIntegrationTests" to true.  Using maven this can be done with a command such as:
 *
 * <code>mvn -DrunAdaptiveCqlIntegrationTests=true verify</code>
 */
public class AdaptiveResultSetTest {

    private final Logger _log = LoggerFactory.getLogger(AdaptiveResultSetTest.class);

    private boolean _runTests;
    private String _keyspaceName;
    private String _tableName;
    private Cluster _cluster;
    private Session _session;

    @Parameters("runAdaptiveCqlIntegrationTests")
    @BeforeClass
    public void setUp(boolean runAdaptiveCqlIntegrationTests) {
        _runTests = runAdaptiveCqlIntegrationTests;
        if (!_runTests) {
            _log.info("All tests in {} will be skipped", getClass());
            return;
        }

        // Create a random keyspace name
        _keyspaceName = "framesizetest" + System.currentTimeMillis() % 10000;
        // Create a table in that keyspace
        _tableName = "framesizetable";

        // Initialize the Cassandra cluster.
        // TODO:  These are hard-coded to match the test environment, but they should be read from a configuration file.
        _cluster = Cluster.builder()
                .addContactPoint("127.0.0.1")
                .withClusterName("emo_cluster")
                .withPort(9164)
                .build();

        _session = _cluster.connect();

        // Create the keyspace specifically for this test
        _session.execute(SchemaBuilder.createKeyspace(_keyspaceName).ifNotExists()
                .with()
                .replication(ImmutableMap.of("class", "NetworkTopologyStrategy", "datacenter1", "1")));

        // Create the table for this test
        _session.execute(SchemaBuilder.createTable(_keyspaceName, _tableName).ifNotExists()
                .addPartitionKey("rowid", DataType.text())
                .addClusteringColumn("col", DataType.bigint())
                .addColumn("data", DataType.blob())
                .withOptions()
                .compactStorage()
                .compactionOptions(SchemaBuilder.sizedTieredStategy()));

        // Populate 10 rows, each with a 137 blobs  The first row, "a" has 4 byte blobs and can be completely queried
        // with a fetch size of 256.  Rows "b" through "k" have 1 megabyte blobs and so will exceed the maximum frame
        // size when queried with a fetch size of 256 since reading two rows requires over 274MB.  137 columns are used
        // to ensure each page ends in the middle of a partition and not on wide row boundaries.

        byte[] bytes = new byte[4];
        ByteBuffer blob = ByteBuffer.wrap(bytes);
        IntBuffer intBuffer = blob.asIntBuffer();

        for (char rowidChar = 'a'; rowidChar <= 'k'; rowidChar++) {
            String rowid = String.valueOf(rowidChar);
            for (int col=0; col < 137; col++) {
                intBuffer.position(0);
                intBuffer.put(col);
                _session.execute(QueryBuilder.insertInto(_keyspaceName, _tableName)
                        .value("rowid", rowid)
                        .value("col", col)
                        .value("data", blob));
            }

            if (rowidChar == 'a') {
                // Grow the blob to 1MB for the remaining rows
                bytes = new byte[(int) Size.megabytes(1).toBytes()];
                blob = ByteBuffer.wrap(bytes);
                intBuffer = blob.asIntBuffer();
            }
        }
    }

    @AfterClass
    public void tearDown() {
        if (!_runTests) {
            return;
        }
        _session.execute(SchemaBuilder.dropKeyspace(_keyspaceName));
        _session.close();
        _cluster.close();
    }

    @DataProvider(name = "async")
    public static Object[][] provideAsyncParameters() {
        return new Object[][] {
                new Object[] { true },
                new Object[] { false }
        };
    }

    @Test(dataProvider = "async")
    public void testInitialPageExceedsMaxFetchSize(boolean async) throws Exception {
        if (!_runTests) {
            throw new SkipException("Skipping test");
        }

        // Query for all data starting with the second row, "b", since the first row can be read in a single frame
        // with fetch size 256.

        Statement statement = QueryBuilder.select("rowid", "col", "data")
                .from(_keyspaceName, _tableName)
                .where(QueryBuilder.gte(QueryBuilder.token("rowid"), "b"))
                .setFetchSize(256);

        // Run once without using adaptive queries to verify the maximum frame size is exceeded
        try {
            _session.execute(statement);
            fail("FrameTooLongException not thrown");
        } catch (FrameTooLongException e) {
            // Ok, this is the expected exception
        }

        ResultSet rs;

        if (async) {
            rs = AdaptiveResultSet.executeAdaptiveQueryAsync(_session, statement, 256).get();
        } else {
            rs = AdaptiveResultSet.executeAdaptiveQuery(_session, statement, 256);
        }

        verifyResults(rs, 'b', 'k');
    }

    @Test(dataProvider = "async")
    public void testSecondPageExceedsMaxFetchSize(boolean async) throws Exception {
        if (!_runTests) {
            throw new SkipException("Skipping test");
        }

        // Query for all data starting with the first row, "a".  The first row has small blobs and therefore the maximum
        // frame size should only be exceeded on the second page.

        Statement statement = QueryBuilder.select("rowid", "col", "data")
                .from(_keyspaceName, _tableName)
                .where(QueryBuilder.gte(QueryBuilder.token("rowid"), "a"))
                .setFetchSize(256);

        // Run once without using adaptive queries to verify the maximum frame size is exceeded only on the second page
        ResultSet rs = _session.execute(statement);
        // Consume the first page's results
        while (rs.getAvailableWithoutFetching() != 0) {
            rs.one();
        }

        // The next read fetches the next page's results which should exceed the maximum frame size
        try {
            rs.one();
            fail("FrameTooLongException not thrown");
        } catch (FrameTooLongException e) {
            // Ok, this is the expected exception
        }

        if (async) {
            rs = AdaptiveResultSet.executeAdaptiveQueryAsync(_session, statement, 256).get();
        } else {
            rs = AdaptiveResultSet.executeAdaptiveQuery(_session, statement, 256);
        }

        verifyResults(rs, 'a', 'k');
    }

    @Test(dataProvider = "async")
    public void testRandomKeysExceedsFetchSize(boolean async) throws Exception {
        if (!_runTests) {
            throw new SkipException("Skipping test");
        }

        // Query for data from 4 rows where each has large blobs

        Statement statement = QueryBuilder.select("rowid", "col", "data")
                .from(_keyspaceName, _tableName)
                .where(QueryBuilder.in("rowid", ImmutableList.of("b", "d", "f", "h")))
                .setFetchSize(256);

        // Run once without using adaptive queries to verify the maximum frame size is exceeded
        try {
            _session.execute(statement);
            fail("FrameTooLongException not thrown");
        } catch (FrameTooLongException e) {
            // Ok, this is the expected exception
        }

        ResultSet rs;

        if (async) {
            rs = AdaptiveResultSet.executeAdaptiveQueryAsync(_session, statement, 256).get();
        } else {
            rs = AdaptiveResultSet.executeAdaptiveQuery(_session, statement, 256);
        }

        Set<String> expectedRowids = Sets.newHashSet("b", "d", "f", "h");
        while (!expectedRowids.isEmpty()) {
            String rowid = verifyNextRow(rs);
            assertTrue(expectedRowids.remove(rowid));
        }

        assertTrue(expectedRowids.isEmpty());
        assertTrue(rs.isExhausted());
    }

    private void verifyResults(ResultSet rs, char from, char to) {
        for (char rowidChar = from; rowidChar <= to; rowidChar++) {
            String rowid = verifyNextRow(rs);
            String expectedRowid = String.valueOf(rowidChar);
            assertEquals(rowid, expectedRowid);
        }

        assertTrue(rs.isExhausted());
    }

    private String verifyNextRow(ResultSet rs) {
        String rowid = null;
        for (int col=0; col < 137; col++) {
            Row row = rs.one();
            assertNotNull(row);

            if (rowid == null) {
                rowid = row.get("rowid", TypeCodec.varchar());
            } else {
                assertEquals(row.get("rowid", TypeCodec.varchar()), rowid);
            }

            assertEquals((long) row.get("col", TypeCodec.bigint()), col);
            assertEquals( row.get("data", TypeCodec.blob()).asIntBuffer().get(), col);
        }

        return rowid;
    }
}
