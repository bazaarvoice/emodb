package com.bazaarvoice.emodb.sor.condition;

import java.util.Collection;

public interface AndCondition extends Condition {

    Collection<Condition> getConditions();
}
