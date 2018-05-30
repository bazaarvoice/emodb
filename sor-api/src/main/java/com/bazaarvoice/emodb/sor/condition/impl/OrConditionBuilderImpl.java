package com.bazaarvoice.emodb.sor.condition.impl;

import com.bazaarvoice.emodb.common.json.OrderedJson;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.condition.ConstantCondition;
import com.bazaarvoice.emodb.sor.condition.EqualCondition;
import com.bazaarvoice.emodb.sor.condition.InCondition;
import com.bazaarvoice.emodb.sor.condition.IntrinsicCondition;
import com.bazaarvoice.emodb.sor.condition.OrCondition;
import com.bazaarvoice.emodb.sor.condition.OrConditionBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class OrConditionBuilderImpl implements OrConditionBuilder {

    private List<Condition> _conditions;
    private List<Object> _values;
    private ConcurrentMap<String, List<Condition>> _intrinsics;
    private boolean _alwaysTrue;

    @Override
    public OrConditionBuilder or(Condition condition) {
        // OR conditions are inherently O(n).  There are a few patterns (eg. Polloi subscriptions) that can lead to
        // long lists of OR conditions and it's worth it to try to convert those conditions to more efficient O(1)
        // lookups using sets.  The optimizations performed here include:
        // 1.  or(..., alwaysTrue(), ...) => alwaysTrue()                     (rare)
        // 2.  or(..., alwaysFalse(), ...) => or(..., ...)                    (rare)
        // 3.  or(equal(val1), equal(val2), ...) => or(in(val1, val2), ...)   (classic O(n) lookup converted to O(1))
        // 4.  or(intrinsic(name:val1), intrinsic(name:val2), ...) =>
        //            or(intrinsic(name:val1,val2), ...)                      (intrinsic O(n) lookup converted to O(1))
        // 5.  or(..., or(...), ...) => or(..., ... ,...)           (flatten OR to increase optimization opportunities)
        //
        // Polloi uses the 4th form.  That's the optimization that has the biggest impact.

        if (condition instanceof ConstantCondition) {
            if (((ConstantCondition) condition).getValue()) {
                _alwaysTrue = true;
            }
            // else ignore alwaysFalse()

        } else if (condition instanceof EqualCondition) {
            if (_values == null) {
                _values = new ArrayList<>();
            }
            _values.add(((EqualCondition) condition).getValue());

        } else if (condition instanceof InCondition) {
            if (_values == null) {
                _values =  new ArrayList<>();
            }
            _values.addAll(((InCondition) condition).getValues());

        } else if (condition instanceof IntrinsicCondition) {
            if (_intrinsics == null) {
                _intrinsics = new ConcurrentHashMap<>();
            }
            IntrinsicCondition intrinsic = (IntrinsicCondition) condition;
            _intrinsics.computeIfAbsent(intrinsic.getName(), ignore -> new ArrayList())
                    .add(intrinsic.getCondition());

        } else if (condition instanceof OrCondition) {
            orAny(((OrCondition) condition).getConditions());

        } else {
            if (_conditions == null) {
                _conditions = new ArrayList<>();
            }
            _conditions.add(condition);
        }
        return this;
    }

    public OrConditionBuilder orAny(Collection<? extends Condition> conditions) {
        for (Condition condition : conditions) {
            or(condition);
        }
        return this;
    }

    @Override
    public Condition build() {
        if (_alwaysTrue) {
            return Conditions.alwaysTrue();
        }

        List<Condition> conditions = new ArrayList<>();
        if (_values != null) {
            conditions.add(Conditions.in(_values));
        }
        if (_intrinsics != null) {
            _intrinsics.entrySet().stream()
                    .sorted(OrderedJson.ENTRY_COMPARATOR)
                    .forEach(e -> conditions.add(Conditions.intrinsic(e.getKey(), Conditions.or(e.getValue()))));
        }
        if (_conditions != null) {
            conditions.addAll(_conditions);
        }

        if (conditions.isEmpty()) {
            return Conditions.alwaysFalse();
        } else if (conditions.size() == 1) {
            return conditions.get(0);
        } else {
            return new OrConditionImpl(conditions);
        }
    }
}
