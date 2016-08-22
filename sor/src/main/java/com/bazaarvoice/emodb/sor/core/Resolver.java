package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.sor.delta.Delta;

import java.util.Set;
import java.util.UUID;

public interface Resolver {

    void update(UUID changeId, Delta delta, Set<String> tags);

    Resolved resolved();
}
