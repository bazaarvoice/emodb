package com.bazaarvoice.emodb.sor.condition;

import java.util.Set;

public interface InCondition extends Condition {

    Set<Object> getValues();
}
