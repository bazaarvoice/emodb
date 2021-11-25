package com.bazaarvoice.emodb.web.cli;

import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

/**
 * Simple Service that performs no actions; useful for DropWizard commands which require a service in the
 * constructor but don't actually use the service.
 */
public class NoOpService<T extends Configuration> extends Application<T> {

    public static <T extends Configuration> NoOpService<T> create() {
        //noinspection unchecked
        return new NoOpService<>();
    }

    private NoOpService() {
        // empty
    }

    @Override
    public void initialize(Bootstrap<T> bootstrap) {
        // no-op
    }

    @Override
    public void run(T configuration, Environment environment)
            throws Exception {
        // no-op
    }
}
