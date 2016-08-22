package com.bazaarvoice.emodb.cachemgr.invalidate;

import com.bazaarvoice.emodb.cachemgr.api.InvalidationEvent;
import com.bazaarvoice.emodb.cachemgr.api.InvalidationScope;

public interface RemoteInvalidationClient {

    void invalidateAll(String endPointAddress, InvalidationScope scope, InvalidationEvent event);
}
