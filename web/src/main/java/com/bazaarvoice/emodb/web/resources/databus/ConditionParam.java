package com.bazaarvoice.emodb.web.resources.databus;

import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import io.dropwizard.jersey.params.AbstractParam;

public class ConditionParam extends AbstractParam<Condition> {

    public ConditionParam(String input) {
        super(input);
    }

    @Override
    protected String errorMessage(Exception e) {
        return "Invalid json-condition string";
    }

    @Override
    protected Condition parse(String input) throws Exception {
        return Conditions.fromString(input);
    }
}
