package com.bazaarvoice.emodb.sor.delta;

import com.bazaarvoice.emodb.sor.condition.Condition;

public interface ConditionalDeltaBuilder extends DeltaBuilder {

    ConditionalDeltaBuilder add(Condition condition, Delta delta);

    ConditionalDeltaBuilder otherwise(Delta delta);
}
