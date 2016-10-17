package com.bazaarvoice.emodb.databus.auth;

import com.bazaarvoice.emodb.databus.model.OwnedSubscription;

/**
 * This interface defines the interactions for authorizing databus subscription and fanout operations.
 * In all cases the ownerId is the internal ID for a user.
 */
public interface DatabusAuthorizer {

    DatabusAuthorizerByOwner owner(String ownerId);

    interface DatabusAuthorizerByOwner {
        /**
         * Checks whether an owner has permission to resubscribe to or poll the provided subscription.  Typically used
         * in response to API subscribe and poll requests, respectively.
         */
        boolean canAccessSubscription(OwnedSubscription subscription);

        /**
         * Checks whether an owner has permission to receive databus events on a given table when polling.  Typically
         * used during fanout to ensure the owner doesn't receive updates for documents he wouldn't have permission to read
         * directly using the DataStore.
         */
        boolean canReceiveEventsFromTable(String table);

    }
}
