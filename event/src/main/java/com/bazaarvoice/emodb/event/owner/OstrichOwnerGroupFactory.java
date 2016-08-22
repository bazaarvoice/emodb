package com.bazaarvoice.emodb.event.owner;

import com.google.common.util.concurrent.Service;
import org.joda.time.Duration;

import javax.annotation.Nullable;

/** Factory for {@link OwnerGroup} instances. */
public interface OstrichOwnerGroupFactory {
    <T extends Service>
    OwnerGroup<T> create(String group, OstrichOwnerFactory<T> factory, @Nullable Duration expireWhenInactive);
}
