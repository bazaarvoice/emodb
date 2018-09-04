package com.bazaarvoice.emodb.common.dropwizard.lifecycle;

import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.google.common.collect.ImmutableMultimap;
import com.google.inject.Inject;
import io.dropwizard.servlets.tasks.Task;

import java.io.PrintWriter;
import java.util.concurrent.Executors;

public class GracefulShutdownTask extends Task {

    private static final String NAME = "graceful-shutdown";

    @Inject
    public GracefulShutdownTask(TaskRegistry taskRegistry) {
        super(NAME);
        taskRegistry.addTask(this);
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter output) throws Exception {
        Executors.newSingleThreadExecutor().submit(() -> System.exit(0));
    }
}
