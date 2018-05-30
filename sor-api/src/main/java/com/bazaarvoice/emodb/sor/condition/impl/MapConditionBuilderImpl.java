package com.bazaarvoice.emodb.sor.condition.impl;

import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.condition.MapConditionBuilder;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class MapConditionBuilderImpl implements MapConditionBuilder {

    private final Map<String, Condition> _entries = new HashMap<>();

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
        if (previous != null) {
            throw new IllegalArgumentException(String.format(
                    "Multiple operations against the same key are not allowed: %s", key));
        }
        return this;
    }

    @Override
    public Condition build() {
        return new MapConditionImpl(_entries);
    }
}
