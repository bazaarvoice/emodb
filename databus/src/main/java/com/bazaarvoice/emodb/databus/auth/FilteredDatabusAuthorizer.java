package com.bazaarvoice.emodb.databus.auth;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Implementation of DatabusAuthorizer that overrides authorization for specific owners.  All other owners
 * are optionally proxied to another instance.
 */
public class FilteredDatabusAuthorizer implements DatabusAuthorizer {

    private final Map<String, DatabusAuthorizer> _ownerOverrides;
    private final DatabusAuthorizer _authorizer;

    private FilteredDatabusAuthorizer(Map<String, DatabusAuthorizer> ownerOverrides,
                                      DatabusAuthorizer authorizer) {
        _ownerOverrides = Objects.requireNonNull(ownerOverrides, "ownerOverrides");
        _authorizer = Objects.requireNonNull(authorizer, "authorizer");
    }

    @Override
    public DatabusAuthorizerByOwner owner(String ownerId) {
        // TODO:  To grandfather in subscriptions before API keys were enforced the following code
        //        always defers to the default authorizer if there is no owner.  This code should be
        //        replaced with the commented-out version once enough time has passed for all grandfathered-in
        //        subscriptions to have been renewed and therefore have an owner attached.
        //
        // return Optional.ofNullable(_ownerOverrides.get(ownerId)).orElse(_authorizer).owner(ownerId);

        DatabusAuthorizer authorizer = null;
        if (ownerId != null) {
            authorizer = _ownerOverrides.get(ownerId);
        }
        if (authorizer == null) {
            authorizer = _authorizer;
        }
        return authorizer.owner(ownerId);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder class for creating a FilteredDatabusAuthorizer.
     */
    public static class Builder {
        private final Map<String, DatabusAuthorizer> _ownerOverrides = Maps.newHashMap();
        private DatabusAuthorizer _defaultAuthorizer;

        private Builder() {
            // no-op
        }

        public Builder withAuthorizerForOwner(String ownerId, DatabusAuthorizer authorizer) {
            checkArgument(!_ownerOverrides.containsKey(ownerId), "Cannot assign multiple rules for owner");
            _ownerOverrides.put(ownerId, authorizer);
            return this;
        }

        public Builder withDefaultAuthorizer(DatabusAuthorizer defaultAuthorizer) {
            checkArgument(_defaultAuthorizer == null, "Cannot assign multiple default authorizers");
            _defaultAuthorizer = defaultAuthorizer;
            return this;
        }

        public FilteredDatabusAuthorizer build() {
            if (_defaultAuthorizer == null) {
                // Unless specified the default behavior is to deny all access to subscriptions and tables
                // not explicitly permitted.
                _defaultAuthorizer = ConstantDatabusAuthorizer.DENY_ALL;
            }
            return new FilteredDatabusAuthorizer(_ownerOverrides, _defaultAuthorizer);
        }
    }
}
