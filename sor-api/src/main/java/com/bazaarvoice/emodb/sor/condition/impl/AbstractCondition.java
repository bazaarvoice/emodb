package com.bazaarvoice.emodb.sor.condition.impl;

import com.bazaarvoice.emodb.common.json.OrderedJson;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.InCondition;
import com.bazaarvoice.emodb.sor.condition.OrCondition;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

public abstract class AbstractCondition implements Condition {

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        try {
            appendTo(buf);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return buf.toString();
    }

    /**
     * Default weight for all conditions is 1.  Conditions which are more complex than a trivial check should return
     * a higher value.
     */
    @Override
    public int weight() {
        return 1;
    }

    protected static void appendSubCondition(Appendable buf, Condition condition) throws IOException {
        // The syntax allows a comma-separated list of conditions that are implicitly wrapped in an OrCondition.
        // If _condition is an InCondition or OrCondition we can print them without the "in(...)" and "or(...)"
        // wrappers to result in a cleaner string format that the parser can parse back into InCondition/OrCondition.
        if (condition instanceof InCondition) {
            Set<Object> values = ((InCondition) condition).getValues();
            Joiner.on(',').appendTo(buf, OrderedJson.orderedStrings(values));
        } else if (condition instanceof OrCondition) {
            Collection<Condition> conditions = ((OrCondition) condition).getConditions();
            String sep = "";
            for (Condition orCondition : conditions) {
                buf.append(sep);
                orCondition.appendTo(buf);
                sep = ",";
            }
        } else {
            condition.appendTo(buf);
        }
    }
}
