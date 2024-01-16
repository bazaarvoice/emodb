package com.bazaarvoice.emodb.auth.jersey;

import com.bazaarvoice.emodb.auth.apikey.ApiKeyAuthenticationTokenGenerator;
import com.google.common.collect.ImmutableList;
import org.apache.shiro.mgt.SecurityManager;

import static java.util.Objects.requireNonNull;

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
        requireNonNull(securityManager, "securityManager");
        ApiKeyAuthenticationTokenGenerator tokenGenerator = new ApiKeyAuthenticationTokenGenerator();

        return new JerseyAuthConfiguration(
                ImmutableList.of(
                        AuthenticationExceptionHandler.class,
                        AuthorizationExceptionHandler.class,
                        UnauthorizedExceptionMapper.class,
                        new AuthenticatedSubjectFeature(securityManager)
                ),
                ImmutableList.of(new AuthDynamicFeature(securityManager, tokenGenerator)));
    }
}
