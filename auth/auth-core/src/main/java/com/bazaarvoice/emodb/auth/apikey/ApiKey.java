package com.bazaarvoice.emodb.auth.apikey;

import com.bazaarvoice.emodb.auth.identity.AuthIdentity;
import com.bazaarvoice.emodb.auth.identity.IdentityState;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

/**
 * {@link AuthIdentity} implementation where identification is performed with a simple API key.
 */
public class ApiKey extends AuthIdentity {

    public ApiKey(String key, String internalId, IdentityState state, Set<String> roles) {
        super(key, internalId, state, roles);
    }

    @JsonCreator
    public ApiKey(@JsonProperty("id") String key,
                  @JsonProperty("internalId") String internalId,
                  @JsonProperty("state") IdentityState state,
                  @JsonProperty("roles") List<String> roles) {
        this(key, internalId, state, ImmutableSet.copyOf(roles));
    }
}
