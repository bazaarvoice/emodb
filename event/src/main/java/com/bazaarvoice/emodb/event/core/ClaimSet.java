package com.bazaarvoice.emodb.event.core;

import java.time.Duration;
import java.util.Collection;

public interface ClaimSet {
    long size();

    boolean isClaimed(byte[] claimId);

    boolean acquire(byte[] claimId, Duration ttl);

    void renew(byte[] claimId, Duration ttl, boolean extendOnly);

    void renewAll(Collection<byte[]> claimIds, Duration ttl, boolean extendOnly);

    void clear();

    void pump();
}
