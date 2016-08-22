package com.bazaarvoice.emodb.web.auth.resource;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Resource which will evaluate as matching all possible inputs.
 */
public class AnyResource extends VerifiableResource {

    @JsonValue
    public String toString() {
        return "*";
    }
}
