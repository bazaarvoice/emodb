package com.bazaarvoice.emodb.event.core;

import com.google.common.base.Function;

import java.util.Map;

public interface ClaimStore {

    <T> T withClaimSet(String name, Function<ClaimSet, T> function);

    Map<String, Long> snapshotClaimCounts();
}
