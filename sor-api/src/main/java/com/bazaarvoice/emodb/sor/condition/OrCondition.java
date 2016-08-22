package com.bazaarvoice.emodb.sor.condition;

import java.util.Collection;

public interface OrCondition extends Condition {

    Collection<Condition> getConditions();
}
