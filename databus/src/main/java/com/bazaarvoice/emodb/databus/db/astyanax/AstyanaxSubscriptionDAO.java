package com.bazaarvoice.emodb.databus.db.astyanax;

import com.bazaarvoice.emodb.common.api.Ttls;
import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.databus.api.DefaultSubscription;
import com.bazaarvoice.emodb.databus.api.Subscription;
import com.bazaarvoice.emodb.databus.db.SubscriptionDAO;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.netflix.astyanax.Execution;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.StringSerializer;
import org.joda.time.Duration;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.netflix.astyanax.model.ConsistencyLevel.CL_LOCAL_QUORUM;

public class AstyanaxSubscriptionDAO implements SubscriptionDAO {

    // all subscriptions are stored as columns of a single row
    private static final String ROW_KEY = "subscriptions";

    private static final ColumnFamily<String, String> CF_SUBSCRIPTION =
            new ColumnFamily<>("subscription",
                    StringSerializer.get(),  // key serializer
                    StringSerializer.get()); // column name serializer

    private final CassandraKeyspace _keyspace;

    @Inject
    public AstyanaxSubscriptionDAO(CassandraKeyspace keyspace) {
        _keyspace = checkNotNull(keyspace, "keyspace");
    }

    @Timed(name = "bv.emodb.databus.AstyanaxSubscriptionDAO.insertSubscription", absolute = true)
    @Override
    public void insertSubscription(String subscription, Condition tableFilter,
                                   Duration subscriptionTtl, Duration eventTtl) {
        Map<String, Object> json = ImmutableMap.<String, Object>of(
                "filter", tableFilter.toString(),
                "expiresAt", System.currentTimeMillis() + subscriptionTtl.getMillis(),
                "eventTtl", Ttls.toSeconds(eventTtl, 1, Integer.MAX_VALUE));
        execute(_keyspace.prepareColumnMutation(CF_SUBSCRIPTION, ROW_KEY, subscription, CL_LOCAL_QUORUM)
                .putValue(JsonHelper.asJson(json), Ttls.toSeconds(subscriptionTtl, 1, Integer.MAX_VALUE)));
    }

    @Timed(name = "bv.emodb.databus.AstyanaxSubscriptionDAO.deleteSubscription", absolute = true)
    @Override
    public void deleteSubscription(String subscription) {
        execute(_keyspace.prepareColumnMutation(CF_SUBSCRIPTION, ROW_KEY, subscription, CL_LOCAL_QUORUM)
                .deleteColumn());
    }

    @Override
    public Subscription getSubscription(String subscription) {
        throw new UnsupportedOperationException();  // CachingSubscriptionDAO should prevent calls to this method.
    }

    @Timed(name = "bv.emodb.databus.AstyanaxSubscriptionDAO.getAllSubscriptions", absolute = true)
    @Override
    public Collection<Subscription> getAllSubscriptions() {
        ColumnList<String> columns = execute(_keyspace.prepareQuery(CF_SUBSCRIPTION, CL_LOCAL_QUORUM)
                .getKey(ROW_KEY));
        List<Subscription> subscriptions = Lists.newArrayListWithCapacity(columns.size());
        for (Column<String> column : columns) {
            String name = column.getName();
            Map<?, ?> json = JsonHelper.fromJson(column.getStringValue(), Map.class);
            Condition tableFilter = Conditions.fromString((String) checkNotNull(json.get("filter"), "filter"));
            Date expiresAt = new Date(((Number) checkNotNull(json.get("expiresAt"), "expiresAt")).longValue());
            Duration eventTtl = Duration.standardSeconds(((Number) checkNotNull(json.get("eventTtl"), "eventTtl")).intValue());
            subscriptions.add(new DefaultSubscription(name, tableFilter, expiresAt, eventTtl));
        }
        return subscriptions;
    }

    private <R> R execute(Execution<R> execution) {
        OperationResult<R> operationResult;
        try {
            operationResult = execution.execute();
        } catch (ConnectionException e) {
            throw Throwables.propagate(e);
        }
        return operationResult.getResult();
    }
}
