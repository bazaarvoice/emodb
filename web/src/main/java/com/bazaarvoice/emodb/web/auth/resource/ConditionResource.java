package com.bazaarvoice.emodb.web.auth.resource;

import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Evaluates whether a resources matches based on a {@link Condition}.  The meaning of the condition and which
 * expressions are valid depends on the context.  For example, an expression with intrinsic conditions is valid
 * when evaluating a table but not a databus subscription.
 */
public class ConditionResource extends VerifiableResource {

    private final Condition _condition;

    @JsonCreator
    public ConditionResource(String condition) {
        this(Conditions.fromString(condition));
    }

    public ConditionResource(Condition condition) {
        _condition = condition;
    }

    public Condition getCondition() {
        return _condition;
    }

    @JsonValue
    public String getConditionString() {
        return _condition.toString();
    }

    @Override
    public String toString() {
        return "if(" + _condition + ")";
    }
}
