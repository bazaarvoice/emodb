package com.bazaarvoice.emodb.sor.condition;

public interface AndConditionBuilder extends ConditionBuilder {

    AndConditionBuilder and(Condition condition);
}
