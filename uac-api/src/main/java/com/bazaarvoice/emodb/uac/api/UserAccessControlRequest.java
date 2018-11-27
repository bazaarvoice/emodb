package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Base class for all non-trivial calls to {@link UserAccessControl}.
 */
abstract public class UserAccessControlRequest {

    private Map<String, Collection<String>> _customRequestParameters = new HashMap<>();

    /**
     * Sets custom request parameters.  Custom parameters may include new features not yet officially supported or
     * additional parameters to existing calls not intended for widespread use.  As such this method is not typically
     * used by most clients.  Furthermore, adding additional parameters may cause the request to fail.
     */
    public void setCustomRequestParameter(String param, String... values) {
        if (_customRequestParameters.get(param) == null) {
            _customRequestParameters.put(param, Arrays.asList(values));
        } else {
            _customRequestParameters.get(param).addAll(Arrays.asList(values));
        }
    }

    @JsonIgnore
    public Map<String, Collection<String>> getCustomRequestParameters() {
        return _customRequestParameters;
    }

}
