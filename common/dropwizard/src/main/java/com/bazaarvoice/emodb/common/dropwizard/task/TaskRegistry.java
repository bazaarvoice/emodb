package com.bazaarvoice.emodb.common.dropwizard.task;

import io.dropwizard.servlets.tasks.Task;

/**
 * Registry of Dropwizard {@link Task} objects.
 */
public interface TaskRegistry {
    void addTask(Task task);
}
