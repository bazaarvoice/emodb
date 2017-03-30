package com.bazaarvoice.emodb.web.resources.databus;

import com.bazaarvoice.emodb.common.json.RisonHelper;
import com.bazaarvoice.emodb.databus.api.DatabusEventTracerSpec;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.google.common.base.Strings;
import io.dropwizard.jersey.params.AbstractParam;

public class TracerParam extends AbstractParam<DatabusEventTracerSpec> {

    public TracerParam(String input) {
        super(input);
    }

    @Override
    protected String errorMessage(String input, Exception e) {
        return "Invalid O-Rison parameter (as described at http://mjtemplate.org/examples/rison.html)" +
                (Strings.isNullOrEmpty(e.getMessage()) ? "" : ", " + e.getMessage()) + ": " + input;
    }

    @Override
    protected DatabusEventTracerSpec parse(String input) throws Exception {
        return RisonHelper.fromORison(input, DatabusEventTracerSpec.class);
    }
}
