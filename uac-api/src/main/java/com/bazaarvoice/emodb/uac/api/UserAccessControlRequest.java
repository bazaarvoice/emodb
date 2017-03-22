package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import java.util.Arrays;

/**
 * Base class for all non-trivial calls to {@link UserAccessControl}.
 */
abstract public class UserAccessControlRequest {

    private Multimap<String, String> _customRequestParameters = ArrayListMultimap.create();

    /**
     * Sets custom request parameters.  Custom parameters may include new features not yet officially supported or
     * additional parameters to existing calls not intended for widespread use.  As such this method is not typically
     * used by most clients.  Furthermore, adding additional parameters may cause the request to fail.
     */
    public void setCustomRequestParameter(String param, String... values) {
        _customRequestParameters.putAll(param, Arrays.asList(values));
    }

    @JsonIgnore
    public Multimap<String, String> getCustomRequestParameters() {
        return _customRequestParameters;
    }

}
