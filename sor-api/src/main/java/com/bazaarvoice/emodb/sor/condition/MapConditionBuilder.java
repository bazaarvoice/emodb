package com.bazaarvoice.emodb.sor.condition;

import javax.annotation.Nullable;
import java.util.Map;

public interface MapConditionBuilder extends ConditionBuilder {

    MapConditionBuilder containsKey(String key);

    MapConditionBuilder contains(String key, @Nullable Object json);

    MapConditionBuilder containsAll(Map<String, ?> json);

    MapConditionBuilder matches(String key, Condition condition);
}
