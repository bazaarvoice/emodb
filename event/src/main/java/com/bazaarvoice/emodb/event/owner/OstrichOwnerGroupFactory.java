package com.bazaarvoice.emodb.event.owner;

import com.google.common.util.concurrent.Service;

import javax.annotation.Nullable;
import java.time.Duration;

/** Factory for {@link OwnerGroup} instances. */
public interface OstrichOwnerGroupFactory {
    <T extends Service>
    OwnerGroup<T> create(String group, OstrichOwnerFactory<T> factory, @Nullable Duration expireWhenInactive);
}
