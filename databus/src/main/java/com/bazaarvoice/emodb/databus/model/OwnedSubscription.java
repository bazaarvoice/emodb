package com.bazaarvoice.emodb.databus.model;

import com.bazaarvoice.emodb.databus.api.Subscription;

/**
 * Extension of {@link Subscription} that includes ownership information not part of the public API that are required
 * for proper maintenance and evaluation.
 */
public interface OwnedSubscription extends Subscription {

    String getOwnerId();
}
