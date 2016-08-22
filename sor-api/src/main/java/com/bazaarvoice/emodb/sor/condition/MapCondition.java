package com.bazaarvoice.emodb.sor.condition;

import java.util.Map;

public interface MapCondition extends Condition {

    Map<String, Condition> getEntries();
}
