package com.bazaarvoice.emodb.auth.apikey;

import com.bazaarvoice.emodb.auth.identity.AuthIdentity;
import com.bazaarvoice.emodb.auth.identity.AuthIdentityModification;
import com.google.common.collect.ImmutableSet;

import static java.util.Objects.requireNonNull;

/**
 * {@link AuthIdentityModification} implementation for API keys.  Since API keys introduce no new attributes to
 * {@link AuthIdentity} this class only defines the two builder methods.
 */
public class ApiKeyModification extends AuthIdentityModification<ApiKey> {

    @Override
    public ApiKey buildNew(String id) {
        requireNonNull(id, "id");
        return buildFrom(new ApiKey(id, getUpdatedRolesFrom(ImmutableSet.of())));
    }

    @Override
    public ApiKey buildFrom(ApiKey identity) {
        requireNonNull(identity, "identity");
        ApiKey apiKey = new ApiKey(identity.getId(), getUpdatedRolesFrom(identity.getRoles()));
        apiKey.setOwner(isOwnerPresent() ? getOwner() : identity.getOwner());
        apiKey.setDescription(isDescriptionPresent() ? getDescription() : identity.getDescription());
        apiKey.setIssued(identity.getIssued());
        apiKey.setMaskedId(identity.getMaskedId());
        return apiKey;
    }
}
