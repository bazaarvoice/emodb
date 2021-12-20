package com.bazaarvoice.emodb.web.resources.databus;

import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.databus.api.PollResult;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * Helper class for asynchronously polling databus resource requests.
 */
public class DatabusResourcePoller {

    private static final Logger _log = LoggerFactory.getLogger(DatabusResourcePoller.class);

    public static final int DEFAULT_NUM_KEEP_ALIVE_THREADS = 4;
    public static final int DEFAULT_NUM_POLLING_THREADS = 8;

    private static final Duration MAX_LONG_POLL_TIME = Duration.ofSeconds(20);
    private static final Duration LONG_POLL_RETRY_TIME = Duration.ofSeconds(2);
    private static final Duration LONG_POLL_SEND_REFRESH_TIME = Duration.ofMillis(200);
    private static final Duration KEEP_ALIVE_SAFETY_BUFFER_TIME = Duration.ofSeconds(10);

    private static final String POLL_DATABUS_EMPTY_HEADER = "X-BV-Databus-Empty";

    private final Timer _pollTimer;

    private final ScheduledExecutorService _keepAliveExecutorService;
    private final ScheduledExecutorService _pollingExecutorService;

    private final Histogram _keepAliveThreadDelayHistogram;
    private final Histogram _pollingThreadDelayHistogram;

    @Inject
    public DatabusResourcePoller(Optional<LongPollingExecutorServices> longPollingExecutorServices,
                                 MetricRegistry metricRegistry) {
        requireNonNull(longPollingExecutorServices, "longPollingExecutorServices");
        if (longPollingExecutorServices.isPresent()) {
            _keepAliveExecutorService = longPollingExecutorServices.get().getKeepAlive();
            _pollingExecutorService = longPollingExecutorServices.get().getPoller();

            addThreadPoolMonitoring(_keepAliveExecutorService, "inactiveKeepAliveThreads", metricRegistry);
            addThreadPoolMonitoring(_pollingExecutorService, "inactivePollingThreads", metricRegistry);
        } else {
            _keepAliveExecutorService = null;
            _pollingExecutorService = null;
        }

        _keepAliveThreadDelayHistogram = metricRegistry.histogram(MetricRegistry.name("bv.emodb.databus", "DatabusResource1", "keepAliveThreadDelay"));
        _pollingThreadDelayHistogram = metricRegistry.histogram(MetricRegistry.name("bv.emodb.databus", "DatabusResource1", "pollingThreadDelay"));

        _pollTimer = buildPollTimer(metricRegistry);
    }

    /**
     * Unit test constructor which does not do long polling.  This is because DropWizard ResourceTest classes
     * do not support asynchronous requests.
     */
    @VisibleForTesting
    public DatabusResourcePoller(MetricRegistry metricRegistry) {
        _pollTimer = buildPollTimer(metricRegistry);
        _keepAliveExecutorService = null;
        _pollingExecutorService = null;
        _keepAliveThreadDelayHistogram = null;
        _pollingThreadDelayHistogram = null;
    }

    // Runnable to intermittently poll for data and output to the response
    private class DatabusPollRunnable implements Runnable {

        private volatile boolean _pollingActive = true;

        private final AsyncContext _asyncContext;
        private KeepAliveRunnable _keepAliveRunnable;
        private Subject _subject;
        private SubjectDatabus _databus;
        private Duration _claimTtl;
        private int _limit;
        private String _subscription;
        private PeekOrPollResponseHelper _helper;
        private long _longPollStopTime;
        private Timer.Context _timerContext;
        private long _lastRunTime = 0;

        DatabusPollRunnable(AsyncContext asyncContext, KeepAliveRunnable keepAliveRunnable, Subject subject, SubjectDatabus databus,
                            Duration claimTtl, int limit, String subscription, PeekOrPollResponseHelper helper,
                            long longPollStopTime, Timer.Context timerContext) {
            _asyncContext = asyncContext;
            _keepAliveRunnable = keepAliveRunnable;
            _subject = subject;
            _databus = databus;
            _claimTtl = claimTtl;
            _limit = limit;
            _subscription = subscription;
            _helper = helper;
            _longPollStopTime = longPollStopTime;
            _timerContext = timerContext;
        }

        @Override
        public void run() {
            boolean rescheduled = false;
            try {
                if (_lastRunTime > 0) {
                    // Record any delay between when we *expected* to run and when we actually ran. This should help
                    // detect overloaded thread pools which, in turn, may lead to timeouts.
                    _pollingThreadDelayHistogram.update(System.currentTimeMillis() - _lastRunTime - LONG_POLL_RETRY_TIME.toMillis());
                }
                if (_pollingActive) {
                    boolean pollFailed = false;
                    PollResult result;
                    try {
                        // Issue a polling call with our server-side client. This ensures that we execute a synchronous
                        // call on the other end and receive a quick response - we do NOT want to execute a long-poll here
                        // and spend up to 20 seconds waiting for a response (occupying this thread all the while).
                        result = _databus.poll(_subject, _subscription, _claimTtl, _limit);
                    } catch (Exception e) {
                        // We're in an async context and have already returned a 200 response.  Since we can't
                        // retroactively change to 500 finish the request with an empty response and log the error.
                        _log.error("Failed to perform asynchronous poll on subscription {}", _subscription, e);
                        result = new PollResult(Collections.emptyIterator(), 0, false);
                        pollFailed = true;
                    }

                    // Go ahead and output the response if we either 1.) find events to output, 2.) exceed our time
                    // limit, or 3.) received an exception during the last poll
                    if (result.getEventIterator().hasNext()
                            || (System.currentTimeMillis() + LONG_POLL_RETRY_TIME.toMillis()) >= _longPollStopTime
                            || pollFailed) {
                        // Lock the context before writing the response to ensure that the KeepAliveRunnable doesn't
                        // insert bad data into the middle of it
                        synchronized (_asyncContext) {
                            if (_pollingActive) {
                                _keepAliveRunnable.cancelKeepAlive();
                                populateResponse(result, (HttpServletResponse) _asyncContext.getResponse(), _helper);
                            }
                        }
                    } else {
                        // Nothing to output; schedule the job to check again.  If the result had more events then poll
                        // again immediately, otherwise wait a few seconds.
                        _lastRunTime = System.currentTimeMillis();
                        if (result.hasMoreEvents()) {
                            _pollingExecutorService.execute(this);
                        } else {
                            _pollingExecutorService.schedule(this, LONG_POLL_RETRY_TIME.toMillis(), TimeUnit.MILLISECONDS);
                        }
                        rescheduled = true;
                    }
                }
            } finally {
                // Stop the timer if we didn't reschedule the job (either because it completed or an exception was thrown)
                if (!rescheduled) {
                    _timerContext.stop();
                    synchronized (_asyncContext) {
                        _asyncContext.complete();
                    }
                }
            }
        }

        public void cancelPolling() {
            _pollingActive = false;
        }
    }

    // Runnable to provide a steady stream of whitespace data to the client to keep our connection alive
    private class KeepAliveRunnable implements Runnable {

        private final AsyncContext _asyncContext;
        private volatile boolean _keepAliveRequired = true;
        private Instant _keepAliveStopTime;
        private DatabusPollRunnable _databusPollRunnable;
        private long _lastRunTime = 0;

        public KeepAliveRunnable(AsyncContext asyncContext, long longPollStopTime) {
            _asyncContext = asyncContext;
            // Allow the keep-alive to run a little longer than the anticipated long-poll cutoff just to be sure that we
            // don't disconnect prematurely in cases where the poll() call takes longer than expected.
            _keepAliveStopTime = Instant.ofEpochMilli(longPollStopTime).plus(KEEP_ALIVE_SAFETY_BUFFER_TIME);
        }

        public void setDatabusPollRunnable(DatabusPollRunnable databusPollRunnable) {
            _databusPollRunnable = databusPollRunnable;
        }

        public void cancelKeepAlive() {
            _keepAliveRequired = false;
        }

        @Override
        public void run() {
            if (_lastRunTime > 0) {
                // Record any delay between when we *expected* to run and when we actually ran. This should help
                // detect overloaded thread pools which, in turn, may lead to timeouts.
                _keepAliveThreadDelayHistogram.update(System.currentTimeMillis() - _lastRunTime - LONG_POLL_SEND_REFRESH_TIME.toMillis());
            }
            // Lock the context before writing to the response to ensure that we don't insert data into it while the
            // DatabusPollRunnable is also writing
            synchronized (_asyncContext) {
                // As long as the keep alive signal is required, send a refresh to the client and schedule another attempt
                // (note that we also cancel out if a certain length of time is exceeded - normally we would rely on the
                // DatabusPollRunnable to kill this Runnable, but this timer serves as a safety blanket in case that job
                // quits unexpectedly)
                if (_keepAliveRequired && _keepAliveStopTime.isAfter(Instant.now())) {
                    try {
                        sendClientRefresh((HttpServletResponse) _asyncContext.getResponse());
                        _lastRunTime = System.currentTimeMillis();
                        _keepAliveExecutorService.schedule(this, LONG_POLL_SEND_REFRESH_TIME.toMillis(), TimeUnit.MILLISECONDS);
                    } catch (Exception ex) {
                        _log.error("Failed to send a keep-alive message to the client");

                        // This exception most likely came from a failure to write the response, meaning that this
                        // request is dead in the water - go ahead and cancel our polling runnable right away so that it
                        // won't issue any poll() requests that might claim data that the client will never receive
                        // (that isn't the end of the world if it happens, but WILL delay the receipt of those messages
                        // in subsequent requests).
                        if (_databusPollRunnable != null) {
                            _databusPollRunnable.cancelPolling();
                        }
                    }
                }
            }
        }
    }

    private static void populateResponse(PollResult result, HttpServletResponse response, PeekOrPollResponseHelper helper) {
        try {
            helper.getJson().writeJson(response.getOutputStream(), result.getEventIterator());
        } catch (IOException ex) {
            _log.error("Failed to write response to the client");
        }
    }

    private static void sendClientRefresh(HttpServletResponse response)
            throws IOException {
        // Only supply whitespace since this won't affect how our results are parsed (consumers should just filter it
        // out, even though it's sufficient to keep our connection alive)
        response.getOutputStream().print(' ');
        response.flushBuffer();
    }

    public Response poll(Subject subject, SubjectDatabus databus, String subscription, Duration claimTtl, int limit, HttpServletRequest request,
                         boolean ignoreLongPoll, PeekOrPollResponseHelper helper) {
        Timer.Context timerContext = _pollTimer.time();
        boolean synchronousResponse = true;
        Response response;

        try {
            // Calculate when the request should internally time out (do this before our first poll() request because we
            // want that to count toward our total run-time)
            long longPollStopTime = System.currentTimeMillis() + MAX_LONG_POLL_TIME.toMillis();

            // Always issue a synchronous request at the start . . this will allow us to bypass our thread pool logic
            // altogether in cases where we might return the value immediately (see more below). There is a danger here
            // that the thread will stall if "databus" is an instance of DatabusClient and we are stuck waiting for a
            // response - however, since we use the server-side client we know that it will always execute synchronously
            // itself (no long-polling) and return in a reasonable period of time.
            PollResult result = databus.poll(subject, subscription, claimTtl, limit);
            if (ignoreLongPoll || result.getEventIterator().hasNext() || _keepAliveExecutorService == null || _pollingExecutorService == null) {
                // If ignoreLongPoll == true or we have no executor services to schedule long-polling on then always
                // return a response, even if it's empty. Alternatively, if we have data to return - return it!
                response = Response.ok()
                        .header(POLL_DATABUS_EMPTY_HEADER, String.valueOf(!result.hasMoreEvents()))
                        .entity(helper.asEntity(result.getEventIterator()))
                        .build();
            } else {
                // If the response is empty then go into async-mode and start up the runnables for our long-polling.
                response = scheduleLongPollingRunnables(request, longPollStopTime, subject, databus, claimTtl, limit, subscription,
                        result.hasMoreEvents(), helper, timerContext);
                synchronousResponse = false;
            }
        } finally {
            // Stop our timer if our request is finished here . . otherwise we are in async mode and it is the
            // responsibility of DatabusPollRunnable to close it out.
            if (synchronousResponse) {
                timerContext.stop();
            }
        }

        return response;
    }

    private Response scheduleLongPollingRunnables(HttpServletRequest request, long longPollStopTime, Subject subject,
                                                  SubjectDatabus databus, Duration claimTtl, int limit, String subscription,
                                                  boolean firstPollHadMoreEvents, PeekOrPollResponseHelper helper,
                                                  Timer.Context timerContext) {
        final AsyncContext ctx = request.startAsync();
        boolean jobsScheduled = false;

        try {
            ctx.getResponse().setContentType("application/json");
            //  We can't know at this point if the queue will be fully drained or not, so base the "databus empty"
            // header on whether the original empty poll had more results.  Worst case the caller has a single false empty
            // header which gets set correctly on the next poll.
            ((HttpServletResponse) ctx.getResponse()).setHeader(POLL_DATABUS_EMPTY_HEADER, String.valueOf(!firstPollHadMoreEvents));

            // Immediately send a client refresh since we've already executed a poll() call and we don't know how long it
            // will take for our scheduled refresh to do its thing. It's probably not needed 99.9% of the time, but we
            // keep it here as a precaution.
            sendClientRefresh((HttpServletResponse) ctx.getResponse());

            KeepAliveRunnable keepAliveRunnable = new KeepAliveRunnable(ctx, longPollStopTime);
            DatabusPollRunnable pollingRunnable = new DatabusPollRunnable(ctx, keepAliveRunnable, subject, databus, claimTtl, limit,
                    subscription, helper, longPollStopTime, timerContext);
            keepAliveRunnable.setDatabusPollRunnable(pollingRunnable);

            // - We don't use ctx.start() here since Jetty's implementation will schedule the job on our request thread pool,
            // which completely nullifies the entire point of using an async implementation
            // - Kick off two recurring jobs, one to poll for events and another to keep the connection to the client alive.
            // Note that we start the keep-alive immediately since we've already taken time above to run an initial poll()
            // request; likewise, we delay before we run another poll() so that we don't run two in immediate succession.
            _pollingExecutorService.schedule(pollingRunnable, LONG_POLL_RETRY_TIME.toMillis(), TimeUnit.MILLISECONDS);
            _keepAliveExecutorService.schedule(keepAliveRunnable, 0, TimeUnit.MILLISECONDS);
            jobsScheduled = true;

            // The actual processing is asynchronous.  However, whatever we return here is included in the result, so make
            // it empty.
            return Response.ok().build();
        } catch (IOException ex) {
            Throwables.throwIfUnchecked(ex);
            throw new RuntimeException(ex);
        } finally {
            // Make sure that we close out the request if our jobs (which would close it for us) failed to get scheduled
            if (!jobsScheduled) {
                ctx.complete();
            }
        }
    }

    private static void addThreadPoolMonitoring(ScheduledExecutorService executorService, String metricName, MetricRegistry metricRegistry) {
        if (executorService instanceof ThreadPoolExecutor) {
            final ThreadPoolExecutor monitoredThreadPoolExecutor = (ThreadPoolExecutor)executorService;
            metricRegistry.register(MetricRegistry.name("bv.emodb.databus", "DatabusResource1", metricName), new Gauge<Integer>() {
                @Override
                public Integer getValue() {
                    return monitoredThreadPoolExecutor.getPoolSize() - monitoredThreadPoolExecutor.getActiveCount();
                }
            });
        }
    }

    private static Timer buildPollTimer(MetricRegistry metricRegistry) {
        return metricRegistry.timer(MetricRegistry.name("bv.emodb.databus", "DatabusResource1", "poll"));
    }
}
