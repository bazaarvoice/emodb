package com.bazaarvoice.emodb.sor.condition;

import javax.annotation.Nullable;

public interface EqualCondition extends Condition {

    @Nullable
    Object getValue();
}
