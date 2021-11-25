package com.bazaarvoice.emodb.web.lifecycle;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ManagedCloseable;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Environment;

import java.io.Closeable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Implementation of {@link LifeCycleRegistry} for Dropwizard {@code Environment} objects.
 */
public class DropwizardLifeCycleRegistry implements LifeCycleRegistry {
    private final Environment _environment;

    @Inject
    public DropwizardLifeCycleRegistry(Environment environment) {
        _environment = checkNotNull(environment, "environment");
    }

    @Override
    public <T extends Managed> T manage(T managed) {
        _environment.lifecycle().manage(managed);
        return managed;
    }

    @Override
    public <T extends Closeable> T manage(T closeable) {
        _environment.lifecycle().manage(new ManagedCloseable(closeable));
        return closeable;
    }
}
