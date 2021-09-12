package com.bazaarvoice.emodb.web.jersey;

import com.google.common.collect.ImmutableList;
import com.sun.jersey.api.model.AbstractMethod;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;
import com.sun.jersey.spi.container.ContainerResponse;
import com.sun.jersey.spi.container.ContainerResponseFilter;
import com.sun.jersey.spi.container.ResourceFilter;
import com.sun.jersey.spi.container.ResourceFilterFactory;

import java.util.List;

/**
 * Resource filter factory ensures that each resource annotated with {@link Unbuffered} will receive the header
 * necessary for {@link UnbufferedStreamFilter} to serve the content unbuffered.
 */
public class UnbufferedStreamResourceFilterFactory implements ResourceFilterFactory, ResourceFilter, ContainerResponseFilter {

    @Override
    public List<ResourceFilter> create(AbstractMethod am) {
        if (am.getResource().getAnnotation(Unbuffered.class) != null || am.getAnnotation(Unbuffered.class) != null) {
            return ImmutableList.of(this);
        }
        return ImmutableList.of();
    }

    @Override
    public ContainerRequestFilter getRequestFilter() {
        return null;
    }

    @Override
    public ContainerResponseFilter getResponseFilter() {
        return this;
    }

    @Override
    public ContainerResponse filter(ContainerRequest request, ContainerResponse response) {
        response.getHttpHeaders().putSingle(UnbufferedStreamFilter.UNBUFFERED_HEADER, "true");
        return response;
    }
}
