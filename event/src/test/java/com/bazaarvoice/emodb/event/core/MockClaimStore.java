package com.bazaarvoice.emodb.event.core;

import com.google.common.base.Function;

import java.util.Collections;
import java.util.Map;

/** No-op implementation of {@link ClaimStore}. */
public class MockClaimStore implements ClaimStore {
    @Override
    public <T> T withClaimSet(String name, Function<ClaimSet, T> function) {
        return function.apply(new DefaultClaimSet());
    }

    @Override
    public Map<String, Long> snapshotClaimCounts() {
        return Collections.emptyMap();
    }
}
