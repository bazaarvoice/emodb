package com.bazaarvoice.emodb.sor.delta;

import com.bazaarvoice.emodb.sor.condition.Condition;

public interface ConditionalDelta extends Delta {

    Condition getTest();

    Delta getThen();

    Delta getElse();
}
