package com.bazaarvoice.emodb.web.resources.sor;

import com.bazaarvoice.emodb.common.json.RisonHelper;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.google.common.base.Strings;
import io.dropwizard.jersey.params.AbstractParam;

public class AuditParam extends AbstractParam<Audit> {

    public AuditParam(String input) {
        super(input);
    }

    @Override
    protected String errorMessage(Exception e) {
        return "Invalid O-Rison parameter (as described at http://mjtemplate.org/examples/rison.html)" +
                (Strings.isNullOrEmpty(e.getMessage()) ? "" : ", " + e.getMessage());
    }

    @Override
    protected Audit parse(String input) throws Exception {
        return RisonHelper.fromORison(input, Audit.class);
    }
}