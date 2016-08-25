package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.databus.api.Subscription;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.sor.condition.eval.ConditionEvaluator;
import com.bazaarvoice.emodb.sor.core.DataProvider;
import com.bazaarvoice.emodb.sor.core.UpdateRef;
import com.bazaarvoice.emodb.table.db.Table;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class SubscriptionEvaluator {
    private static final Logger _log = LoggerFactory.getLogger(SubscriptionEvaluator.class);

    private final DataProvider _dataProvider;
    private final RateLimitedLog _rateLimitedLog;

    @Inject
    public SubscriptionEvaluator(DataProvider dataProvider,
                                 RateLimitedLogFactory logFactory) {
        _dataProvider = dataProvider;
        _rateLimitedLog = logFactory.from(_log);
    }

    public <S extends Subscription> Iterable<S> matches(Iterable<S> subscriptions, final MatchEventData eventData) {
        return FluentIterable.from(subscriptions)
                .filter(new Predicate<Subscription>() {
                    @Override
                    public boolean apply(Subscription subscription) {
                        return matches(subscription, eventData);
                    }
                });
    }

    public boolean matches(Subscription subscription, ByteBuffer eventData) {
        MatchEventData matchEventData;
        try {
            matchEventData = getMatchEventData(eventData);
        } catch (UnknownTableException e) {
            return false;
        }

        return matches(subscription, matchEventData);
    }

    public boolean matches(Subscription subscription, MatchEventData eventData) {
        Table table = eventData.getTable();
        try {
            Map<String, Object> json;
            if (eventData.getTags().isEmpty()) {
                json = table.getAttributes();
            } else {
                json = Maps.newHashMap(table.getAttributes());
                json.put(UpdateRef.TAGS_NAME, eventData.getTags());
            }
            return ConditionEvaluator.eval(subscription.getTableFilter(), json, new TableFilterIntrinsics(table));
        } catch (Exception e) {
            _rateLimitedLog.error(e, "Unable to evaluate condition for subscription " + subscription.getName() +
                    " on table {}: {}", table.getName(), subscription.getTableFilter());
            return false;
        }
    }

    public MatchEventData getMatchEventData(ByteBuffer eventData) throws UnknownTableException {
        UpdateRef ref = UpdateRefSerializer.fromByteBuffer(eventData.duplicate());
        return new MatchEventData(_dataProvider.getTable(ref.getTable()), ref.getTags());
    }

    protected class MatchEventData {
        private final Table _table;
        private final Set<String> _tags;

        public MatchEventData(Table table, Set<String> tags) {
            _table = checkNotNull(table, "table");
            _tags = tags;
        }

        public Table getTable() {
            return _table;
        }

        public Set<String> getTags() {
            return _tags;
        }
    }
}
