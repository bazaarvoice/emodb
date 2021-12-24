package com.bazaarvoice.emodb.web.task;

import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.google.inject.Inject;
import io.dropwizard.servlets.tasks.Task;
import io.dropwizard.setup.Environment;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of {@link TaskRegistry} for Dropwizard {@code Environment} objects.
 */
public class DropwizardTaskRegistry implements TaskRegistry {
    private final Environment _environment;

    @Inject
    public DropwizardTaskRegistry(Environment environment) {
        _environment = requireNonNull(environment, "environment");
    }

    @Override
    public void addTask(Task task) {
        _environment.admin().addTask(task);
    }
}
