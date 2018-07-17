package com.bazaarvoice.emodb.databus.db;

import com.bazaarvoice.emodb.databus.model.OwnedSubscription;
import com.bazaarvoice.emodb.sor.condition.Condition;

import javax.annotation.Nullable;
import java.time.Duration;

public interface SubscriptionDAO {

    void insertSubscription(String ownerId, String subscription, Condition tableFilter, Duration subscriptionTtl,
                            Duration eventTtl);

    void deleteSubscription(String subscription);

    @Nullable
    OwnedSubscription getSubscription(String subscription);

    Iterable<OwnedSubscription> getAllSubscriptions();

    /**
     * Potentially more efficient than {@link #getAllSubscriptions()} when the caller only needs a list of all
     * subscription names.  If possible the implementation should provide a more efficient implementation than
     * actually loading all subscriptions.
     */
    Iterable<String> getAllSubscriptionNames();
}
