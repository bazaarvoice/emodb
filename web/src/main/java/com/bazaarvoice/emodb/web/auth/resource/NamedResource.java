package com.bazaarvoice.emodb.web.auth.resource;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import static java.util.Objects.requireNonNull;

/**
 * Matches a single resource by name.  The meaning of the name depends on the context.  For example, in the
 * system-of-record it is the table name whereas in the databus it is a subscription.
 */
public class NamedResource extends VerifiableResource {

    private final String _name;

    @JsonCreator
    public NamedResource(String name) {
        _name = requireNonNull(name);
    }

    @JsonValue
    public String getName() {
        return _name;
    }

    @Override
    public String toString() {
        return _name;
    }
}
