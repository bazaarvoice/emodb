package com.bazaarvoice.emodb.sor.condition.impl;

import com.bazaarvoice.emodb.common.json.OrderedJson;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.ConditionVisitor;
import com.bazaarvoice.emodb.sor.condition.InCondition;
import com.bazaarvoice.emodb.sor.condition.IntrinsicCondition;
import com.bazaarvoice.emodb.sor.condition.OrCondition;
import com.bazaarvoice.emodb.sor.delta.deser.DeltaJson;
import com.google.common.base.Joiner;
import com.google.common.io.CharStreams;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class IntrinsicConditionImpl extends AbstractCondition implements IntrinsicCondition {

    private final String _name;
    private final Condition _condition;

    public IntrinsicConditionImpl(String name, Condition condition) {
        _name = checkNotNull(name, "name");
        _condition = checkNotNull(condition, "condition");
        checkArgument(Intrinsic.DATA_FIELDS.contains(name), name);
        checkArgument(!Intrinsic.VERSION.equals(name), name);
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
        Writer out = CharStreams.asWriter(buf);
        DeltaJson.write(out, _name);
        buf.append(':');
        // The syntax allows a comma-separated list of conditions that are implicitly wrapped in an OrCondition.
        // If _condition is an InCondition or OrCondition we can print them without the "in(...)" and "or(...)"
        // wrappers to result in a cleaner string format that the parser can parse back into InCondition/OrCondition.
        if (_condition instanceof InCondition) {
            Set<Object> values = ((InCondition) _condition).getValues();
            Joiner.on(',').appendTo(buf, OrderedJson.orderedStrings(values));
        } else if (_condition instanceof OrCondition) {
            Collection<Condition> conditions = ((OrCondition) _condition).getConditions();
            String sep = "";
            for (Condition condition : conditions) {
                buf.append(sep);
                condition.appendTo(buf);
                sep = ",";
            }
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
