package com.bazaarvoice.emodb.databus.auth;

import com.bazaarvoice.emodb.databus.model.OwnedSubscription;

/**
 * Simple {@link DatabusAuthorizer} implementation that either permits or denies all requests based on the provided
 * value.
 */
public class ConstantDatabusAuthorizer implements DatabusAuthorizer {

    private final ConstantDatabusAuthorizerForOwner _authorizer;

    public static final ConstantDatabusAuthorizer ALLOW_ALL = new ConstantDatabusAuthorizer(true);
    public static final ConstantDatabusAuthorizer DENY_ALL = new ConstantDatabusAuthorizer(false);

    private ConstantDatabusAuthorizer(boolean authorize) {
        _authorizer = new ConstantDatabusAuthorizerForOwner(authorize);
    }

    @Override
    public DatabusAuthorizerByOwner owner(String ownerId) {
        return _authorizer;
    }

    private class ConstantDatabusAuthorizerForOwner implements DatabusAuthorizerByOwner {
        private final boolean _authorize;

        private ConstantDatabusAuthorizerForOwner(boolean authorize) {
            _authorize = authorize;
        }

        @Override
        public boolean canAccessSubscription(OwnedSubscription subscription) {
            return _authorize;
        }

        @Override
        public boolean canReceiveEventsFromTable(String table) {
            return _authorize;
        }
    }
}
