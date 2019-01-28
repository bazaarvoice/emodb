package com.bazaarvoice.emodb.common.dropwizard.jersey;

import java.io.IOException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.DynamicFeature;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.FeatureContext;


/**
 * Resource filter ensures that each resource annotated with {@link Unbuffered} will receive the header
 * necessary for {@link UnbufferedStreamFilter} to serve the content unbuffered.
 */

public class UnbufferedStreamDynamicFeature implements DynamicFeature {
    @Override
    public void configure(ResourceInfo resourceInfo, FeatureContext context) {
        if (resourceInfo.getResourceClass().getAnnotation(Unbuffered.class) != null
                || resourceInfo.getResourceMethod().getAnnotation(Unbuffered.class) != null) {
            context.register(UnbufferedStreamResourceFilter.class);
        }
    }

    private static class UnbufferedStreamResourceFilter implements ContainerResponseFilter {
        @Override
        public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) throws IOException {
            responseContext.getHeaders().putSingle(UnbufferedStreamFilter.UNBUFFERED_HEADER, "true");
        }
    }
}