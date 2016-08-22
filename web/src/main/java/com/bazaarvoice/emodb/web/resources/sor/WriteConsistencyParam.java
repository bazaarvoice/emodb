package com.bazaarvoice.emodb.web.resources.sor;

import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import io.dropwizard.jersey.params.AbstractParam;

public class WriteConsistencyParam extends AbstractParam<WriteConsistency> {

    public WriteConsistencyParam(String input) {
        super(input);
    }

    @Override
    protected WriteConsistency parse(String input) throws Exception {
        return WriteConsistency.valueOf(input.toUpperCase());
    }
}
