package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.condition.eval.ConditionEvaluator;
import com.bazaarvoice.emodb.sor.core.DataProvider;
import com.bazaarvoice.emodb.table.db.Table;
import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.Map;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.TopicNameExtractor;

public class SubscriptionTopicNameExtractor implements TopicNameExtractor<String, Map<String, Object>> {

    private final DataProvider _dataProvider;

    public SubscriptionTopicNameExtractor(DataProvider dataProvider) {
        _dataProvider = dataProvider;
    }

    @Override
    public Collection<String> extract(String key, Map<String, Object> value, RecordContext recordContext) {
        Table table = _dataProvider.getTable((String) value.get("~table"));
        System.out.println(value);
        System.out.println(table.getAttributes());

        if (ConditionEvaluator.eval(Conditions.mapBuilder().contains("type", "review").build(), table.getAttributes(),
                new SubscriptionIntrinsics(table, (String) value.get("~id")))) {
            return ImmutableSet.of("review", "ugc");
        }

        return ImmutableSet.of("ugc");

    }
}
