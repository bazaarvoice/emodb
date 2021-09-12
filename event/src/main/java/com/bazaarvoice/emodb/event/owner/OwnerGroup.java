package com.bazaarvoice.emodb.event.owner;

import com.google.common.util.concurrent.Service;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.time.Duration;
import java.util.Map;

/**
 * A group of services that should only run when the current server is considered the owner of the specified object.
 */
public interface OwnerGroup<T extends Service> extends Closeable {
    /**
     * Returns a started service for the specified object, or absent if this server is not the owner of the object.
     * This is idempotent--if the service has been started already, it will be returned.
     * @param name the name of the object to start a service for.
     * @param waitDuration the amount of time to wait for the service to startup if the service needs to be started.
     */
    @Nullable
    T startIfOwner(String name, Duration waitDuration);

    /** Returns a snapshot of the current set of active services. */
    Map<String, T> getServices();

    /** Stops the selected service if it is started.  Returns immediately without waiting. */
    void stop(String name);

    /** Stops all services. */
    void close();
}
