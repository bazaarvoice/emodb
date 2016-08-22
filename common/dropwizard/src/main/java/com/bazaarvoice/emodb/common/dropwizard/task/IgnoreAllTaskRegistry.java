package com.bazaarvoice.emodb.common.dropwizard.task;

import io.dropwizard.servlets.tasks.Task;

/**
 * Implementation of TaskRegistry that ignores all registration requests.
 */
public class IgnoreAllTaskRegistry implements TaskRegistry {
    @Override
    public void addTask(Task task) {
        // ignore
    }
}
