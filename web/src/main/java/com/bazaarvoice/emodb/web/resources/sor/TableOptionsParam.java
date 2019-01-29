package com.bazaarvoice.emodb.web.resources.sor;

import com.bazaarvoice.emodb.common.json.RisonHelper;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.google.common.base.Strings;
import io.dropwizard.jersey.params.AbstractParam;

public class TableOptionsParam extends AbstractParam<TableOptions> {

    public TableOptionsParam(String input) {
        super(input);
    }

    @Override
    protected String errorMessage(Exception e) {
        return "Invalid O-Rison parameter (as described at http://mjtemplate.org/examples/rison.html)" +
                (Strings.isNullOrEmpty(e.getMessage()) ? "" : ", " + e.getMessage());
    }

    @Override
    protected TableOptions parse(String input) throws Exception {
        return RisonHelper.fromORison(input, TableOptions.class);
    }
}