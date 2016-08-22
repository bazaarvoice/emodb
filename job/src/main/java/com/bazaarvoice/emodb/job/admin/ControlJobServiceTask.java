package com.bazaarvoice.emodb.job.admin;

import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.job.api.JobService;
import com.google.common.collect.ImmutableMultimap;
import com.google.inject.Inject;
import io.dropwizard.servlets.tasks.Task;

import java.io.PrintWriter;

/**
 * This class registers two DropWizard tasks which can be used to start and stop background job processing on this
 * server.
 * <ul>
 *     <li><em>stop-job-processing</em> stops any new jobs in the job queue from running on this server.</li>
 *     <li><em>start-job-processing</em> restarts running jobs from the job queue after a previous stop.</li>
 * </ul>
 * <p>
 * Nodes:
 * <ul>
 *     <li>When job processing is stopped any jobs already running at the time are not affected.</li>
 *     <li>Stopping job processing only affects the local server.  To stop all job processing this call
 *         must be repeated on every EmoDB instance.</li>
 * </ul>
 * Usage:
 * <pre>
 * curl -s -XPOST http://localhost:8081/tasks/stop-job-processing
 * curl -s -XPOST http://localhost:8081/tasks/start-job-processing
 * </pre>
 */
public class ControlJobServiceTask {

    private final JobService _jobService;

    @Inject
    public ControlJobServiceTask(TaskRegistry taskRegistry, JobService jobService) {
        _jobService = jobService;

        taskRegistry.addTask(new Task("stop-job-processing") {
            @Override
            public void execute(ImmutableMultimap<String, String> parameters, PrintWriter printWriter)
                    throws Exception {
                stopJobProcessing(printWriter);
            }
        });

        taskRegistry.addTask(new Task("start-job-processing") {
            @Override
            public void execute(ImmutableMultimap<String, String> parameters, PrintWriter printWriter)
                    throws Exception {
                startJobProcessing(printWriter);
            }
        });
    }

    private void stopJobProcessing(PrintWriter printWriter) {
        if (_jobService.pause()) {
            printWriter.print("Job processing has been stopped");
        } else {
            printWriter.print("Job processing remains stopped (no change)");
        }
    }

    private void startJobProcessing(PrintWriter printWriter) {
        if (_jobService.resume()) {
            printWriter.print("Job processing has been started");
        } else {
            printWriter.print("Job processing remains started (no change)");
        }
    }
}
