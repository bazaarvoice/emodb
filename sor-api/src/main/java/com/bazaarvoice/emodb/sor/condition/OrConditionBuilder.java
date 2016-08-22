package com.bazaarvoice.emodb.sor.condition;

public interface OrConditionBuilder extends ConditionBuilder {

    OrConditionBuilder or(Condition condition);
}
