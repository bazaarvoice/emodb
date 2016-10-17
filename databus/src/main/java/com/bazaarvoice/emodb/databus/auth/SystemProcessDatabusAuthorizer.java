package com.bazaarvoice.emodb.databus.auth;

import com.bazaarvoice.emodb.databus.model.OwnedSubscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link DatabusAuthorizer} for system processes, such as the canary and databus replay.
 */
public class SystemProcessDatabusAuthorizer implements DatabusAuthorizer {

    private final Logger _log = LoggerFactory.getLogger(getClass());

    private final String _systemOwnerId;

    private final DatabusAuthorizerByOwner _processAuthorizer = new DatabusAuthorizerByOwner() {
        @Override
        public boolean canAccessSubscription(OwnedSubscription subscription) {
            // System should only access its own subscriptions
            return _systemOwnerId.equals(subscription.getOwnerId());
        }

        @Override
        public boolean canReceiveEventsFromTable(String table) {
            // System needs to be able to poll on updates to all tables
            return true;
        }
    };

    public SystemProcessDatabusAuthorizer(String systemOwnerId) {
        _systemOwnerId = checkNotNull(systemOwnerId, "systemOwnerId");
    }

    @Override
    public DatabusAuthorizerByOwner owner(String ownerId) {
        if (_systemOwnerId.equals(ownerId)) {
            return _processAuthorizer;
        }
        _log.warn("Non-system owner attempted authorization from system authorizer: {}", ownerId);
        return ConstantDatabusAuthorizer.DENY_ALL.owner(ownerId);
    }
}
