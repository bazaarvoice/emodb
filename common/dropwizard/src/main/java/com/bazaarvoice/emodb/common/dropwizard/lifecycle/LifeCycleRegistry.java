package com.bazaarvoice.emodb.common.dropwizard.lifecycle;

import io.dropwizard.lifecycle.Managed;

import java.io.Closeable;

/**
 * Registry that promises to call {@link Managed#start()} and  {@link Managed#stop()} at the appropriate time for
 * each registered instance of {@link Managed}.
 */
public interface LifeCycleRegistry {
    <T extends Managed> T manage(T managed);
    <T extends Closeable> T manage(T closeable);
}
