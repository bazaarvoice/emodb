package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.common.api.ServiceUnavailableException;
import com.bazaarvoice.emodb.web.partition.PartitionForwardingException;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import javax.ws.rs.ext.Providers;
import java.security.SecureRandom;
import java.util.Random;

/**
 * Exception mapper for when a partition-aware request is forwarded to another server and communication with that
 * server fails.  Don't propagate the root cause exception, which is likely meaningless to the caller, but rather
 * advise the caller to try again shortly.
 */
@Provider
public class PartitionForwardingExceptionMapper implements ExceptionMapper<PartitionForwardingException> {

    private final Providers _providers;

    public PartitionForwardingExceptionMapper(@Context Providers providers) {
        _providers = providers;
    }

    @SuppressWarnings("ThrowableNotThrown")
    @Override
    public Response toResponse(PartitionForwardingException exception) {

        // To prevent herding advise the caller to retry after 1 to 5 seconds, chosen randomly.
        return _providers.getExceptionMapper(ServiceUnavailableException.class)
                .toResponse(new ServiceUnavailableException("Service unavailable, try again later", new SecureRandom().nextInt(5) + 1));
    }
}
