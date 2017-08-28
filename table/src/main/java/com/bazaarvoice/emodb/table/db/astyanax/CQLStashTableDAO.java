package com.bazaarvoice.emodb.table.db.astyanax;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.inject.Inject;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class is used to create and query a snapshot of tables for Stash.  For example, if a table is deleted, moved or
 * has its metadata altered mid-Stash the output should still include the content as it would have appeared at the beginning
 * of Stash.  It also provides an efficient API for querying which tables in that snapshot fall within a specific
 * token range.
 *
 * Since this class and {@link com.bazaarvoice.emodb.table.db.TableDAO} both provide low level access to tables here's an
 * explanation for why Stash-related table operations have been split from that class.  First, it's cleaner to separate
 * the concerns for Stash from the needs of general table management, which are quite complex in their own right.
 * Furthermore, TableDAO is used for both system of record and blob tables while Stash only applies to the former, so
 * keeping the Stash portion here reinforces this separation.  For these reasons TableDAO focuses on general table management
 * and delegates Stash operations to this class.  Consequently this class works exclusively with the serializable form of
 * tables, {@link TableJson}, leaving the domain knowledge of converting these back into tables to TableDAO.
 */
public class CQLStashTableDAO {
    
    private final static String STASH_TOKEN_RANGE_TABLE = "stash_token_range";
    // No Stash run should take over 1 day, so set the TTL to clean up stash tables if they aren't explicitly cleaned by then,
    // plus a small buffer.
    private final static int TTL = (int) TimeUnit.HOURS.toSeconds(28);

    private final String _systemTablePlacement;
    private final PlacementCache _placementCache;
    private final DataCenters _dataCenters;
    private volatile boolean _verifiedStashTokenRangeTableExists;

    @Inject
    public CQLStashTableDAO(@SystemTablePlacement String systemTablePlacement,
                            PlacementCache placementCache, DataCenters dataCenters) {
        _systemTablePlacement = checkNotNull(systemTablePlacement, "systemTablePlacement");
        _placementCache = checkNotNull(placementCache, "placementCache");
        _dataCenters = checkNotNull(dataCenters, "dataCenters");
    }

    public void addTokenRangesForTable(String stashId, AstyanaxStorage readStorage, TableJson tableJson) {
        String placement = readStorage.getPlacementName();
        ensureStashTokenRangeTableExists();

        String tableInfo = JsonHelper.asJson(tableJson.getRawJson());
        BatchStatement batchStatement = new BatchStatement();

        // Add two records for each shard for the table: one which identifies the start token for the shard, and
        // one that identifies (exclusively) the end token for the shard.  This will allow for efficient range queries
        // later on.
        readStorage.scanIterator(null).forEachRemaining(range -> {
            batchStatement.add(QueryBuilder.insertInto(STASH_TOKEN_RANGE_TABLE)
                    .value("stash_id", stashId)
                    .value("data_center", _dataCenters.getSelf().getName())
                    .value("placement", placement)
                    .value("range_token", range.getStart())
                    .value("is_start_token", true)
                    .value("table_json", tableInfo));

            batchStatement.add(QueryBuilder.insertInto(STASH_TOKEN_RANGE_TABLE)
                    .value("stash_id", stashId)
                    .value("data_center", _dataCenters.getSelf().getName())
                    .value("placement", placement)
                    .value("range_token", range.getEnd())
                    .value("is_start_token", false)
                    .value("table_json", tableInfo));
        });

        _placementCache.get(_systemTablePlacement)
                .getKeyspace()
                .getCqlSession()
                .execute(batchStatement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM));
    }

    public Iterator<ProtoStashTokenRange> getTokenRangesBetween(String stashId, String placement, ByteBuffer fromInclusive, ByteBuffer toExclusive) {
        ensureStashTokenRangeTableExists();

        // Because of the way the stash token range table is laid out the query range must cross the start or end token for a
        // shard in order to be included in the results.  If the entire query range is for the same table and shard then
        // querying the table naively won't return any results, even if it matches a table.
        //
        // To account for this first check if the query range is within a single shard, then perform the appropriate
        // query based on the result.
        if (fromSameShard(fromInclusive, toExclusive)) {
            return getTokenRangesBetweenIntraShard(stashId, placement, fromInclusive, toExclusive);
        } else {
            return getTokenRangesBetweenInterShard(stashId, placement, fromInclusive, toExclusive);
        }
    }

    /**
     * Two tokens are from the same shard if the following are both true:
     * <ol>
     *     <li>Both tokens are at least 9 bytes long (1 shard + 8 table uuid bytes)</li>
     *     <li>The shard and table uuid for both tokens are identical</li>
     * </ol>
     */
    private boolean fromSameShard(ByteBuffer fromInclusive, ByteBuffer toExclusive) {
        return fromInclusive.remaining() >= 9 &&
                toExclusive.remaining() >= 9 &&
                RowKeyUtils.getShardId(fromInclusive) == RowKeyUtils.getShardId(toExclusive) &&
                RowKeyUtils.getTableUuid(fromInclusive) == RowKeyUtils.getTableUuid(toExclusive);
    }
    
    private Iterator<ProtoStashTokenRange> getTokenRangesBetweenIntraShard(String stashId, String placement,
                                                                           ByteBuffer fromInclusive, ByteBuffer toExclusive) {
        // Since the range falls entirely within a single shard run a targeted query that only looks for the beginning
        // of that shard.

        ByteBuffer startToken = RowKeyUtils.getRowKeyRaw(RowKeyUtils.getShardId(fromInclusive), RowKeyUtils.getTableUuid(fromInclusive), new byte[0]);

        ResultSet resultSet = _placementCache.get(_systemTablePlacement)
                .getKeyspace()
                .getCqlSession()
                .execute(
                        QueryBuilder.select("table_json")
                                .from(STASH_TOKEN_RANGE_TABLE)
                                .where(QueryBuilder.eq("stash_id", stashId))
                                .and(QueryBuilder.eq("data_center", _dataCenters.getSelf().getName()))
                                .and(QueryBuilder.eq("placement", placement))
                                .and(QueryBuilder.eq("range_token", startToken))
                                .and(QueryBuilder.eq("is_start_token", true))
                                .limit(1)
                                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM));

        Row row = resultSet.one();

        if (row == null) {
            return Iterators.emptyIterator();
        }

        TableJson tableJson = toTableJson(row.getString(0));
        return Iterators.singletonIterator(new ProtoStashTokenRange(fromInclusive, toExclusive, tableJson));
    }

    private Iterator<ProtoStashTokenRange> getTokenRangesBetweenInterShard(String stashId, String placement,
                                                                           ByteBuffer fromInclusive, ByteBuffer toExclusive) {
        // Since the range crosses the boundary between at least two tables it is guaranteed that if any table has a shard
        // whose token range intersects the query range then by querying for all start and end tokens within that range
        // either the start token, end token, or both will be returned in the results.

        ResultSet resultSet = _placementCache.get(_systemTablePlacement)
                .getKeyspace()
                .getCqlSession()
                .execute(
                        QueryBuilder.select("range_token", "is_start_token", "table_json")
                                .from(STASH_TOKEN_RANGE_TABLE)
                                .where(QueryBuilder.eq("stash_id", stashId))
                                .and(QueryBuilder.eq("data_center", _dataCenters.getSelf().getName()))
                                .and(QueryBuilder.eq("placement", placement))
                                .and(QueryBuilder.gte("range_token", fromInclusive))
                                .and(QueryBuilder.lt("range_token", toExclusive))
                                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                                .setFetchSize(100));

        final Iterator<Row> resultSetIterator = resultSet.iterator();

        return new AbstractIterator<ProtoStashTokenRange>() {
            TableJson currentTable;
            ByteBuffer currentTableStartToken;

            @Override
            protected ProtoStashTokenRange computeNext() {
                ProtoStashTokenRange range = null;

                while (range == null) {
                    if (resultSetIterator.hasNext()) {
                        Row row = resultSetIterator.next();
                        TableJson table = getTableJson(row);
                        if (isStartToken(row)) {
                            if (currentTable == null) {
                                currentTable = table;
                                currentTableStartToken = getToken(row);
                            } else {
                                throw new IllegalStateException("Overlapping table range start rows found");
                            }
                        } else if (currentTable == null) {
                            // We're starting in the middle of a table's token range.
                            range = new ProtoStashTokenRange(fromInclusive, getToken(row), table);
                        } else if (currentTable.getTable().equals(table.getTable())) {
                            range = new ProtoStashTokenRange(currentTableStartToken, getToken(row), table);
                            currentTable = null;
                            currentTableStartToken = null;
                        } else {
                            throw new IllegalStateException("Overlapping table range end rows found");
                        }
                    } else if (currentTable != null) {
                        // Were ending in the middle of a table's token range.
                        range = new ProtoStashTokenRange(currentTableStartToken, toExclusive, currentTable);
                        currentTable = null;
                        currentTableStartToken = null;
                    } else {
                        return endOfData();
                    }
                }

                return range;
            }

            private ByteBuffer getToken(Row row) {
                return row.getBytesUnsafe(0);
            }

            private boolean isStartToken(Row row) {
                return row.getBool(1);
            }

            private TableJson getTableJson(Row row) {
                return toTableJson(row.getString(2));
            }
        };
    }

    private TableJson toTableJson(String tableJsonString) {
        Map<String, Object> tableJsonMap = JsonHelper.fromJson(tableJsonString, new TypeReference<Map<String, Object>>() {});
        return new TableJson(tableJsonMap);
    }

    public void clearTokenRanges(String stashId) {
        ensureStashTokenRangeTableExists();

        _placementCache.get(_systemTablePlacement)
                .getKeyspace()
                .getCqlSession()
                .execute(
                        QueryBuilder.delete()
                                .from(STASH_TOKEN_RANGE_TABLE)
                                .where(QueryBuilder.eq("stash_id", stashId))
                                .and(QueryBuilder.eq("data_center", _dataCenters.getSelf().getName()))
                                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM));
    }

    private void ensureStashTokenRangeTableExists() {
        if (!_verifiedStashTokenRangeTableExists) {
            synchronized(this) {
                if (!_verifiedStashTokenRangeTableExists) {
                    // Primary key is ((stash_id, data_center), placement, range_token, is_start_token).
                    // Note that Cassandra performs unsigned byte comparison for "range_token" and sorts False before
                    // True for "is_start_token".  The latter is necessary because it sorts two tables with
                    // adjacent UUIDs correctly, returning the exclusive "to" token for the previous table before the
                    // inclusive "from" token for the next table.
                    _placementCache.get(_systemTablePlacement)
                            .getKeyspace()
                            .getCqlSession()
                            .execute(SchemaBuilder.createTable(STASH_TOKEN_RANGE_TABLE)
                                    .ifNotExists()
                                    .addPartitionKey("stash_id", DataType.text())
                                    .addPartitionKey("data_center", DataType.text())
                                    .addClusteringColumn("placement", DataType.text())
                                    .addClusteringColumn("range_token", DataType.blob())
                                    .addClusteringColumn("is_start_token", DataType.cboolean())
                                    .addColumn("table_json", DataType.text())
                                    .withOptions()
                                    .compactStorage()
                                    .defaultTimeToLive(TTL));

                    _verifiedStashTokenRangeTableExists = true;
                }
            }
        }
    }

    /**
     * Similar to {@link com.bazaarvoice.emodb.table.db.stash.StashTokenRange} except it utilizes TableJson.  It is
     * up to the caller to convert that serialized form into a Table.
     */
    public static class ProtoStashTokenRange {
        private ByteBuffer _from;
        private ByteBuffer _to;
        private TableJson _tableJson;

        public ProtoStashTokenRange(ByteBuffer from, ByteBuffer to, TableJson tableJson) {
            _from = from;
            _to = to;
            _tableJson = tableJson;
        }

        public ByteBuffer getFrom() {
            return _from;
        }

        public ByteBuffer getTo() {
            return _to;
        }

        public TableJson getTableJson() {
            return _tableJson;
        }
    }
}
