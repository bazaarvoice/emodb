package com.bazaarvoice.emodb.sor.condition.impl;

import com.bazaarvoice.emodb.common.json.OrderedJson;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.ConditionVisitor;
import com.bazaarvoice.emodb.sor.condition.InCondition;
import com.bazaarvoice.emodb.sor.condition.IntrinsicCondition;
import com.bazaarvoice.emodb.sor.condition.OrCondition;
import com.bazaarvoice.emodb.sor.delta.deser.DeltaJson;
import com.bazaarvoice.emodb.streaming.AppendableJoiner;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class IntrinsicConditionImpl extends AbstractCondition implements IntrinsicCondition {

    private final String _name;
    private final Condition _condition;

    public IntrinsicConditionImpl(String name, Condition condition) {
        _name = requireNonNull(name, "name");
        _condition = requireNonNull(condition, "condition");
        if (!Intrinsic.DATA_FIELDS.contains(name) || Intrinsic.VERSION.equals(name)) {
            throw new IllegalArgumentException(name);
        }
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public Condition getCondition() {
        return _condition;
    }

    @Override
    public <T, V> V visit(ConditionVisitor<T, V> visitor, @Nullable T context) {
        return visitor.visit(this, context);
    }

    @Override
    public void appendTo(Appendable buf) throws IOException {
        buf.append("intrinsic(");
        DeltaJson.append(buf, _name);
        buf.append(':');
        // The syntax allows a comma-separated list of conditions that are implicitly wrapped in an OrCondition.
        // If _condition is an InCondition or OrCondition we can print them without the "in(...)" and "or(...)"
        // wrappers to result in a cleaner string format that the parser can parse back into InCondition/OrCondition.
        if (_condition instanceof InCondition) {
            Set<Object> values = ((InCondition) _condition).getValues();
            OrderedJson.orderedStrings(values).stream().collect(AppendableJoiner.joining(buf, ","));
        } else if (_condition instanceof OrCondition) {
            Collection<Condition> conditions = ((OrCondition) _condition).getConditions();
            conditions.stream().collect(AppendableJoiner.joining(buf, ",", (app, cond) -> cond.appendTo(app)));
        } else {
            _condition.appendTo(buf);
        }
        buf.append(')');
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IntrinsicCondition)) {
            return false;
        }
        IntrinsicCondition that = (IntrinsicCondition) o;
        return _name.equals(that.getName()) &&
                _condition.equals(that.getCondition());
    }

    @Override
    public int hashCode() {
        return 31 * _name.hashCode() + _condition.hashCode() + 757;
    }
}
