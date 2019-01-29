package com.bazaarvoice.emodb.web.throttling;

import javax.ws.rs.container.ContainerRequestContext;

/**
 * Base interface for providing a {@link ConcurrentRequestRegulator} based on the request.  Typically the regulator
 * returned is based on the request method and path.
 */
public interface ConcurrentRequestRegulatorSupplier {

    ConcurrentRequestRegulator forRequest(ContainerRequestContext request);
}
