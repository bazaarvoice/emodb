package com.bazaarvoice.emodb.sor.condition;

public interface LikeCondition extends Condition {

    boolean matches(String input);
}
