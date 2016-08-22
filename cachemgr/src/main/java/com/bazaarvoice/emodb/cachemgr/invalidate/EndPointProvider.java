package com.bazaarvoice.emodb.cachemgr.invalidate;

import com.google.common.base.Function;

import java.util.Collection;

public interface EndPointProvider {

    void withEndPoints(Function<Collection<EndPoint>, ?> function);
}
