package com.bazaarvoice.emodb.databus.db;

import com.bazaarvoice.emodb.databus.api.Subscription;
import com.bazaarvoice.emodb.sor.condition.Condition;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.Collection;

public interface SubscriptionDAO {

    void insertSubscription(String subscription, Condition tableFilter, Duration subscriptionTtl, Duration eventTtl);

    void deleteSubscription(String subscription);

    @Nullable
    Subscription getSubscription(String subscription);

    Collection<Subscription> getAllSubscriptions();
}
