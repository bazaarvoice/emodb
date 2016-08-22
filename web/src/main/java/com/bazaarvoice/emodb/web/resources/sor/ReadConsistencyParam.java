package com.bazaarvoice.emodb.web.resources.sor;

import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import io.dropwizard.jersey.params.AbstractParam;

public class ReadConsistencyParam extends AbstractParam<ReadConsistency> {

    public ReadConsistencyParam(String input) {
        super(input);
    }

    @Override
    protected ReadConsistency parse(String input) throws Exception {
        return ReadConsistency.valueOf(input.toUpperCase());
    }
}
