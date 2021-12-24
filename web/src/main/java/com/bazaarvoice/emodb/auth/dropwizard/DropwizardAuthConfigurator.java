package com.bazaarvoice.emodb.auth.dropwizard;

import com.bazaarvoice.emodb.auth.jersey.JerseyAuthConfiguration;
import com.bazaarvoice.emodb.auth.jersey.JerseyAuthConfigurationBuilder;
import com.sun.jersey.spi.container.ResourceFilterFactory;
import io.dropwizard.setup.Environment;
import org.apache.shiro.mgt.SecurityManager;

import static java.util.Objects.requireNonNull;

/**
 * This class can be used to configure security and any Jersey resources within a DropWizard framework so that requests
 * can authenticated and/or authorized using @{@link org.apache.shiro.authz.annotation.RequiresAuthentication} and
 * @{@link org.apache.shiro.authz.annotation.RequiresPermissions} annotations.  This should be called within
 * {@link io.dropwizard.Application#run(io.dropwizard.Configuration, io.dropwizard.setup.Environment)}.
 * For example:
 *
 * <code>
 *     AuthIdentityManager&lt;ApiKey&gt; identityManager = ...;
 *     PermissionManager permissionManager = ...;
 *
 *     SecurityManager securityManager = SecurityManagerBuilder.create()
 *             .withRealmName("MyRealm")
 *             .withAuthIdentityReader(identityManager)
 *             .withPermissionReader(permissionManager)
 *             .build();
 *
 *     new DropWizardAuthConfigurator(securityManager).configure(environment);
 * </code>
 */
public class DropwizardAuthConfigurator {

    private final SecurityManager _securityManager;

    public DropwizardAuthConfigurator(SecurityManager securityManager) {
        _securityManager = securityManager;
    }

    public void configure(Environment environment) {
        requireNonNull(environment, "environment");
        JerseyAuthConfiguration config = JerseyAuthConfigurationBuilder.build(_securityManager);

        for (ResourceFilterFactory resourceFilterFactory : config.getResourceFilterFactories()) {
            //noinspection unchecked
            environment.jersey().getResourceConfig().getResourceFilterFactories().add(resourceFilterFactory);
        }
        for (Object provider : config.getProviderInstances()) {
            environment.jersey().register(provider);
        }
        for (Class<?> providerClass : config.getProviderClasses()) {
            environment.jersey().register(providerClass);
        }
    }
}
