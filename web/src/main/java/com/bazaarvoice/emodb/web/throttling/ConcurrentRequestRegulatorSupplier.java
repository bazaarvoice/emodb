package com.bazaarvoice.emodb.web.throttling;

import com.sun.jersey.spi.container.ContainerRequest;

/**
 * Base interface for providing a {@link ConcurrentRequestRegulator} based on the request.  Typically the regulator
 * returned is based on the request method and path.
 */
public interface ConcurrentRequestRegulatorSupplier {

    ConcurrentRequestRegulator forRequest(ContainerRequest request);
}
