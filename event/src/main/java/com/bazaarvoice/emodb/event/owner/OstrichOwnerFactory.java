package com.bazaarvoice.emodb.event.owner;

import com.bazaarvoice.ostrich.PartitionContext;
import com.google.common.util.concurrent.Service;

public interface OstrichOwnerFactory<T extends Service> {
    /** Return the partition context used for the consistent hashing calculation. */
    PartitionContext getContext(String name);

    /** Create the service that owns the specified name.  The service will be started and stopped by the caller. */
    T create(String name);
}
