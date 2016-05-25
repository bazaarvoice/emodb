package com.bazaarvoice.emodb.auth.apikey;

import com.bazaarvoice.emodb.auth.identity.AuthIdentity;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.Hashing;

import java.util.List;
import java.util.Set;

/**
 * {@link AuthIdentity} implementation where identification is performed with a simple API key.
 */
public class ApiKey extends AuthIdentity {

    public ApiKey(String key, String internalId, Set<String> roles) {
        super(key, internalId, roles);
    }

    @JsonCreator
    public ApiKey(@JsonProperty("id") String key,
                  @JsonProperty("internalId") String internalId,
                  @JsonProperty("roles") List<String> roles) {

        // API keys have been in use since before internal IDs were introduced.  To grandfather in those keys we'll
        // use a hash of the API key as the internal ID.
        this(key, resolveInternalId(key, internalId), ImmutableSet.copyOf(roles));
    }

    private static String resolveInternalId(String key, String internalId) {
        if (internalId != null) {
            return internalId;
        }
        // SHA-256 is a little heavy but it has two advantages:
        // 1.  It is the same algorithm currently used to store API keys by the permission manager so there isn't a
        //     potential conflict between keys.
        // 2.  The API keys are cached by Shiro so this conversion will only take place when the key needs to
        //     be (re)loaded.
        return Hashing.sha256().hashUnencodedChars(key).toString();
    }
}
