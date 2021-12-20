package com.bazaarvoice.emodb.databus.db.cql;

import com.bazaarvoice.emodb.common.api.Ttls;
import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.databus.db.SubscriptionDAO;
import com.bazaarvoice.emodb.databus.model.DefaultOwnedSubscription;
import com.bazaarvoice.emodb.databus.model.OwnedSubscription;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.codahale.metrics.annotation.Timed;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.time.Clock;
import java.time.Duration;
import java.util.Date;
import java.util.Map;
import java.util.stream.StreamSupport;

import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.ttl;
import static java.util.Objects.requireNonNull;

public class CqlSubscriptionDAO implements SubscriptionDAO {

    // all subscriptions are stored as columns of a single row
    private static final String ROW_KEY = "subscriptions";

    private static final String CF_NAME = "subscription";

    private final CassandraKeyspace _keyspace;
    private final Clock _clock;
    private String _rowkeyColumn;
    private String _subscriptionNameColumn;
    private String _subscriptionColumn;

    @Inject
    public CqlSubscriptionDAO(CassandraKeyspace keyspace, Clock clock) {
        _keyspace = requireNonNull(keyspace, "keyspace");
        _clock = requireNonNull(clock, "clock");
    }

    @Timed(name = "bv.emodb.databus.CqlSubscriptionDAO.insertSubscription", absolute = true)
    @Override
    public void insertSubscription(String ownerId, String subscription, Condition tableFilter,
                                   Duration subscriptionTtl, Duration eventTtl) {
        Map<String, Object> json = ImmutableMap.<String, Object>builder()
                .put("filter", tableFilter.toString())
                .put("expiresAt", _clock.millis() + subscriptionTtl.toMillis())
                .put("eventTtl", Ttls.toSeconds(eventTtl, 1, Integer.MAX_VALUE))
                .put("ownerId", ownerId)
                .build();

        _keyspace.getCqlSession().execute(
                insertInto(CF_NAME)
                        .value(rowkeyColumn(), ROW_KEY)
                        .value(subscriptionNameColumn(), subscription)
                        .value(subscriptionColumn(), JsonHelper.asJson(json))
                        .using(ttl(Ttls.toSeconds(subscriptionTtl, 1, Integer.MAX_VALUE)))
                        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM));
    }

    @Timed(name = "bv.emodb.databus.CqlSubscriptionDAO.deleteSubscription", absolute = true)
    @Override
    public void deleteSubscription(String subscription) {
        _keyspace.getCqlSession().execute(
                delete()
                        .from(CF_NAME)
                        .where(eq(rowkeyColumn(), ROW_KEY))
                        .and(eq(subscriptionNameColumn(), subscription))
                        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM));
    }

    @Timed(name = "bv.emodb.databus.CqlSubscriptionDAO.getSubscription", absolute = true)
    @Override
    public OwnedSubscription getSubscription(String subscription) {
        ResultSet resultSet = _keyspace.getCqlSession().execute(
                select(subscriptionNameColumn(), subscriptionColumn())
                        .from(CF_NAME)
                        .where(eq(rowkeyColumn(), ROW_KEY))
                        .and(eq(subscriptionNameColumn(), subscription))
                        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM));

        Row row = resultSet.one();
        if (row == null) {
            return null;
        }
        return rowToOwnedSubscription(row);
    }

    @Timed(name = "bv.emodb.databus.CqlSubscriptionDAO.getAllSubscriptions", absolute = true)
    @Override
    public Iterable<OwnedSubscription> getAllSubscriptions() {
        return () -> {
            ResultSet resultSet = _keyspace.getCqlSession().execute(
                    select(subscriptionNameColumn(), subscriptionColumn())
                            .from(CF_NAME)
                            .where(eq(rowkeyColumn(), ROW_KEY))
                            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                            .setFetchSize(200));

            return StreamSupport.stream(resultSet.spliterator(), false).map(this::rowToOwnedSubscription).iterator();
        };
    }

    private OwnedSubscription rowToOwnedSubscription(Row row) {
        String name = row.getString(0);
        Map<?, ?> json = JsonHelper.fromJson(row.getString(1), Map.class);
        Condition tableFilter = Conditions.fromString((String) requireNonNull(json.get("filter"), "filter"));
        Date expiresAt = new Date(((Number) requireNonNull(json.get("expiresAt"), "expiresAt")).longValue());
        Duration eventTtl = Duration.ofSeconds(((Number) requireNonNull(json.get("eventTtl"), "eventTtl")).intValue());
        // TODO:  Once API keys are fully integrated enforce non-null
        String ownerId = (String) json.get("ownerId");
        return new DefaultOwnedSubscription(name, tableFilter, expiresAt, eventTtl, ownerId);
    }

    @Timed(name = "bv.emodb.databus.CqlSubscriptionDAO.getAllSubscriptionNames", absolute = true)
    @Override
    public Iterable<String> getAllSubscriptionNames() {
        return () -> {
            ResultSet resultSet = _keyspace.getCqlSession().execute(
                    select(subscriptionNameColumn())
                            .from(CF_NAME)
                            .where(eq(rowkeyColumn(), ROW_KEY))
                            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                            .setFetchSize(5000));

            return StreamSupport.stream(resultSet.spliterator(), false).map(row -> row.getString(0)).iterator();
        };
    }

    private String rowkeyColumn() {
        if (_rowkeyColumn == null) {
            getColumnNames();
        }
        return _rowkeyColumn;
    }

    private String subscriptionNameColumn() {
        if (_subscriptionNameColumn == null) {
            getColumnNames();
        }
        return _subscriptionNameColumn;
    }

    private String subscriptionColumn() {
        if (_subscriptionColumn == null) {
            getColumnNames();
        }
        return _subscriptionColumn;
    }

    /**
     * Because of the way databus tables were created historically using Astyanax and Cassandra 1.2 there may be
     * inconsistency in the names of the CQL columns in the subscription table.  To be safe read the table metadata
     * to get the column names.
     */
    private void getColumnNames() {
        TableMetadata table = _keyspace.getKeyspaceMetadata().getTable(CF_NAME);
        _rowkeyColumn = table.getPrimaryKey().get(0).getName();
        _subscriptionNameColumn = table.getPrimaryKey().get(1).getName();
        _subscriptionColumn = table.getColumns().get(2).getName();
    }
}
