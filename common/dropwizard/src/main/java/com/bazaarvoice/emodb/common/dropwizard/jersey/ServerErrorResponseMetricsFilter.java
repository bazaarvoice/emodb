package com.bazaarvoice.emodb.common.dropwizard.jersey;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerResponse;
import com.sun.jersey.spi.container.ContainerResponseFilter;

import javax.ws.rs.core.Response;

/**
 * By default DropWizard includes a metric tracking all 5xx errors returned by the application:
 * "io.dropwizard.jetty.MutableServletContextHandler.5xx-responses".  However, it is frequently useful to
 * track 5xx responses by specific response code.  For example, a spike in 500 errors may indicate a different issue
 * from a spike in 503 errors and it may be desirable to tune logging and alerting around these independently.
 *
 * This filter maintains metrics for 500 and 503 responses independently.  All other 5xx responses are grouped together
 * in a third "other" metric since as of this writing none are explicitly returned by EmoDB.  There is no metric
 * which counts all 5xx errors since that would be redundant with the DropWizard metric.
 *
 * @see org.eclipse.jetty.server.handler.StatisticsHandler
 */
public class ServerErrorResponseMetricsFilter implements ContainerResponseFilter {

    private final Meter _meter500;
    private final Meter _meter503;
    private final Meter _meterOther;
    
    public ServerErrorResponseMetricsFilter(MetricRegistry metricRegistry) {
        _meter500 = metricRegistry.meter(MetricRegistry.name("bv.emodb.web", "500-responses"));
        _meter503 = metricRegistry.meter(MetricRegistry.name("bv.emodb.web", "503-responses"));
        _meterOther = metricRegistry.meter(MetricRegistry.name("bv.emodb.web", "5xx-other-responses"));
    }

    @Override
    public ContainerResponse filter(ContainerRequest request, ContainerResponse response) {
        if (response.getStatusType().getFamily() == Response.Status.Family.SERVER_ERROR) {
            switch (response.getStatus()) {
                case 500:
                    _meter500.mark();
                    break;
                case 503:
                    _meter503.mark();
                    break;
                default:
                    _meterOther.mark();
                    break;
            }
        }
        return response;
    }
}
