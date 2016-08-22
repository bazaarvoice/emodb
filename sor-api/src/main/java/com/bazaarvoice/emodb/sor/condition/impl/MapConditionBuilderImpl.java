package com.bazaarvoice.emodb.sor.condition.impl;

import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.condition.MapConditionBuilder;
import com.google.common.collect.Maps;

import javax.annotation.Nullable;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class MapConditionBuilderImpl implements MapConditionBuilder {

    private final Map<String, Condition> _entries = Maps.newHashMap();

    @Override
    public MapConditionBuilder containsKey(String key) {
        return matches(key, Conditions.isDefined());
    }

    @Override
    public MapConditionBuilder contains(String key, @Nullable Object json) {
        return matches(key, Conditions.equal(json));
    }

    @Override
    public MapConditionBuilder containsAll(Map<String, ?> json) {
        for (Map.Entry<String, ?> entry : json.entrySet()) {
            contains(entry.getKey(), entry.getValue());
        }
        return this;
    }

    @Override
    public MapConditionBuilder matches(String key, Condition condition) {
        Condition previous = _entries.put(key, condition);
        checkArgument(previous == null, "Multiple operations against the same key are not allowed: %s", key);
        return this;
    }

    @Override
    public Condition build() {
        return new MapConditionImpl(_entries);
    }
}
