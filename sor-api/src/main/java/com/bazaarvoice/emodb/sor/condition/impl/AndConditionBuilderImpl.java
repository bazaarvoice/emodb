package com.bazaarvoice.emodb.sor.condition.impl;

import com.bazaarvoice.emodb.sor.condition.AndConditionBuilder;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

public class AndConditionBuilderImpl implements AndConditionBuilder {

    private final List<Condition> _conditions = Lists.newArrayList();

    @Override
    public AndConditionBuilder and(Condition condition) {
        _conditions.add(condition);
        return this;
    }

    public AndConditionBuilder andAll(Collection<? extends Condition> conditions) {
        _conditions.addAll(conditions);
        return this;
    }

    @Override
    public Condition build() {
        if (_conditions.isEmpty()) {
            return Conditions.alwaysTrue();
        } else if (_conditions.size() == 1) {
            return _conditions.iterator().next();
        } else {
            return new AndConditionImpl(_conditions);
        }
    }
}
