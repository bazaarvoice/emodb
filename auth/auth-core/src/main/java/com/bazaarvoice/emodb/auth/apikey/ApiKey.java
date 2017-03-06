package com.bazaarvoice.emodb.auth.apikey;

import com.bazaarvoice.emodb.auth.identity.AuthIdentity;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

/**
 * {@link AuthIdentity} implementation where identification is performed with a simple API key.
 */
public class ApiKey extends AuthIdentity {

    public ApiKey(String internalId, Set<String> roles) {
        super(internalId, roles);
    }

    @JsonCreator
    public ApiKey(@JsonProperty("internalId") String internalId,
                  @JsonProperty("roles") List<String> roles) {

        this(internalId, ImmutableSet.copyOf(roles));
    }
}
