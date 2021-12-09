package com.bazaarvoice.emodb.web.auth.jersey;

import com.bazaarvoice.emodb.web.auth.apikey.ApiKeyAuthenticationTokenGenerator;
import com.google.common.collect.ImmutableList;
import com.sun.jersey.spi.container.ResourceFilterFactory;
import org.apache.shiro.mgt.SecurityManager;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class can be used to  get the Jersey ResourceFactoryFilters and Providers required to authenticate and/or
 * authorize requests using @{@link org.apache.shiro.authz.annotation.RequiresAuthentication} and
 * @{@link org.apache.shiro.authz.annotation.RequiresPermissions} annotations.  It is up to the caller to apply
 * the returned values to a Jersey server.
 */
 public class JerseyAuthConfigurationBuilder {

    private JerseyAuthConfigurationBuilder() {
        // empty
    }

    public static JerseyAuthConfiguration build(SecurityManager securityManager) {
        checkNotNull(securityManager, "securityManager");
        ApiKeyAuthenticationTokenGenerator tokenGenerator = new ApiKeyAuthenticationTokenGenerator();

        return new JerseyAuthConfiguration(
                ImmutableList.of(
                        AuthenticationExceptionHandler.class,
                        AuthorizationExceptionHandler.class,
                        UnauthorizedExceptionMapper.class,
                        new AuthenticatedSubjectProvider(securityManager)
                ),
                ImmutableList.<ResourceFilterFactory>of(new AuthResourceFilterFactory(securityManager, tokenGenerator))
        );
    }
}
