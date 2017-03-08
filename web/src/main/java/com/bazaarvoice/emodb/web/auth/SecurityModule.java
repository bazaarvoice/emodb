package com.bazaarvoice.emodb.web.auth;

import com.bazaarvoice.emodb.auth.AuthCacheRegistry;
import com.bazaarvoice.emodb.auth.EmoSecurityManager;
import com.bazaarvoice.emodb.auth.InternalAuthorizer;
import com.bazaarvoice.emodb.auth.SecurityManagerBuilder;
import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.dropwizard.DropwizardAuthConfigurator;
import com.bazaarvoice.emodb.auth.identity.AuthIdentityManager;
import com.bazaarvoice.emodb.auth.identity.CacheManagingAuthIdentityManager;
import com.bazaarvoice.emodb.auth.identity.DeferringAuthIdentityManager;
import com.bazaarvoice.emodb.auth.identity.TableAuthIdentityManager;
import com.bazaarvoice.emodb.auth.permissions.CacheManagingPermissionManager;
import com.bazaarvoice.emodb.auth.permissions.DeferringPermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionManager;
import com.bazaarvoice.emodb.auth.permissions.TablePermissionManager;
import com.bazaarvoice.emodb.auth.shiro.GuavaCacheManager;
import com.bazaarvoice.emodb.auth.shiro.InvalidatableCacheManager;
import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.databus.ReplicationKey;
import com.bazaarvoice.emodb.databus.SystemInternalId;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.compactioncontrol.CompControlApiKey;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.inject.Exposed;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.PermissionResolver;
import org.apache.shiro.mgt.SecurityManager;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Guice module which configures security on the local server.
 *  * <p>
 * Requires the following external references:
 * <ul>
 * <li> {@link AuthorizationConfiguration}
 * <li> {@link DataStore}
 * <li> {@link com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry}
 * <li> @{@link com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort} HostAndPort
 * <li> @{@link com.bazaarvoice.emodb.common.dropwizard.guice.ServerCluster} String
 * </ul>
 * Exports the following:
 * <ul>
 * <li> {@link DropwizardAuthConfigurator}
 * <li> @{@link ReplicationKey} String
 * <li> @{@link CompControlApiKey} String
 * <li> @{@link SystemInternalId} String
 * <li> {@link PermissionResolver}
 * <li> {@link InternalAuthorizer}
 * </ul>
 */
public class SecurityModule extends PrivateModule {

    private final static String REALM_NAME = "EmoDB";
    private final static String ANONYMOUS_KEY = "anonymous";

    // Internal identifiers for reserved API keys
    private final static String ADMIN_INTERNAL_ID = "__admin";
    private final static String REPLICATION_INTERNAL_ID = "__replication";
    private final static String ANONYMOUS_INTERNAL_ID = "__anonymous";

    // Internal identifier for reserved internal processes that do not have a public facing API key
    private final static String SYSTEM_INTERNAL_ID = "__system";

    @Override
    protected void configure() {
        bind(HashFunction.class).annotatedWith(ApiKeyHashFunction.class).toInstance(Hashing.sha256());
        bind(ApiKeyEncryption.class).asEagerSingleton();
        bind(ApiKeyAdminTask.class).asEagerSingleton();
        bind(RoleAdminTask.class).asEagerSingleton();

        bind(new TypeLiteral<Set<String>>() {})
                .annotatedWith(ReservedRoles.class)
                .toInstance(ImmutableSet.of(
                        DefaultRoles.replication.toString(),
                        DefaultRoles.anonymous.toString()));

        bind(PermissionResolver.class).to(EmoPermissionResolver.class).asEagerSingleton();
        bind(SecurityManager.class).to(EmoSecurityManager.class);
        bind(InternalAuthorizer.class).to(EmoSecurityManager.class);

        bind(String.class).annotatedWith(SystemInternalId.class).toInstance(SYSTEM_INTERNAL_ID);

        expose(DropwizardAuthConfigurator.class);
        expose(Key.get(String.class, ReplicationKey.class));
        expose(Key.get(String.class, CompControlApiKey.class));
        expose(Key.get(String.class, SystemInternalId.class));
        expose(PermissionResolver.class);
        expose(InternalAuthorizer.class);
    }

    @Provides
    @Singleton
    @Inject
    EmoSecurityManager provideSecurityManager(
            AuthIdentityManager<ApiKey> authIdentityManager,
            PermissionManager permissionManager,
            InvalidatableCacheManager cacheManager,
            @Named("AnonymousKey") Optional<String> anonymousKey) {

        return SecurityManagerBuilder.create()
                .withRealmName(REALM_NAME)
                .withAuthIdentityManager(authIdentityManager)
                .withPermissionManager(permissionManager)
                .withAnonymousAccessAs(anonymousKey.orNull())
                .withCacheManager(cacheManager)
                .build();
    }

    @Provides
    @Singleton
    DropwizardAuthConfigurator provideDropwizardAuthConfigurator(SecurityManager securityManager) {
        return new DropwizardAuthConfigurator(securityManager);
    }

    @Provides
    @Singleton
    @ReplicationKey
    String provideReplicationKey(AuthorizationConfiguration config, ApiKeyEncryption encryption) {
        return configurationKeyAsPlaintext(config.getReplicationApiKey(), encryption, "replication");
    }

    @Provides
    @Singleton
    @CompControlApiKey
    String provideCompControlKey(AuthorizationConfiguration config, ApiKeyEncryption encryption) {
        return configurationKeyAsPlaintext(config.getCompControlApiKey(), encryption, "compaction-control");
    }

    @Provides
    @Singleton
    @Exposed
    @Named("AdminKey")
    String provideAdminKey(AuthorizationConfiguration config, ApiKeyEncryption encryption) {
        return configurationKeyAsPlaintext(config.getAdminApiKey(), encryption, "admin");
    }

    private String configurationKeyAsPlaintext(String key, ApiKeyEncryption encryption, String description) {
        try {
            return encryption.decrypt(key);
        } catch (Exception e) {
            // If it looks at all like it was intended to be encrypted then propagate the exception
            if (ApiKeyEncryption.isPotentiallyEncryptedApiKey(key)) {
                throw e;
            }

            // Warn that they really should encrypt the key, but otherwise allow it.
            LoggerFactory.getLogger("com.bazaarvoice.emodb.security").warn(
                    "Configuration key {} is stored in plaintext; anyone with access to config.yaml can see it!!!", description);
            return key;
        }
    }

    @Provides
    @Singleton
    @Named("AnonymousKey")
    Optional<String> provideAnonymousKey(AuthorizationConfiguration config) {
        if (config.isAllowAnonymousAccess()) {
            return Optional.of(ANONYMOUS_KEY);
        }
        return Optional.absent();
    }

    @Provides
    @Singleton
    @Named("dao")
    AuthIdentityManager<ApiKey> provideAuthIdentityManagerDAO(
            AuthorizationConfiguration config, DataStore dataStore, @ApiKeyHashFunction HashFunction hash) {
        return new TableAuthIdentityManager<>(ApiKey.class, dataStore, config.getIdentityTable(),
                config.getInternalIdIndexTable(), config.getTablePlacement(), hash);
    }

    @Provides
    @Singleton
    @Inject
    AuthIdentityManager<ApiKey> provideAuthIdentityManager(
            @ReplicationKey String replicationKey,
            InvalidatableCacheManager cacheManager,
            @Named("AdminKey") String adminKey, @Named("AnonymousKey") Optional<String> anonymousKey,
            @Named("dao") AuthIdentityManager<ApiKey> daoManager) {

        ImmutableList.Builder<ApiKey> reservedIdentities = ImmutableList.builder();
        reservedIdentities.add(
                new ApiKey(replicationKey, REPLICATION_INTERNAL_ID, ImmutableSet.of(DefaultRoles.replication.toString())),
                new ApiKey(adminKey, ADMIN_INTERNAL_ID, ImmutableSet.of(DefaultRoles.admin.toString())));

        if (anonymousKey.isPresent()) {
            reservedIdentities.add(new ApiKey(anonymousKey.get(), ANONYMOUS_INTERNAL_ID, ImmutableSet.of(DefaultRoles.anonymous.toString())));
        }

        AuthIdentityManager<ApiKey> deferring = new DeferringAuthIdentityManager<>(daoManager, reservedIdentities.build());

        return new CacheManagingAuthIdentityManager<>(deferring, cacheManager);
    }

    @Provides
    @Singleton
    @Named("dao")
    PermissionManager providePermissionManagerDAO(
            AuthorizationConfiguration config, PermissionResolver permissionResolver, DataStore dataStore) {
        return new TablePermissionManager(
                permissionResolver, dataStore, config.getPermissionsTable(), config.getTablePlacement());
    }

    @Provides
    @Singleton
    @Inject
    InvalidatableCacheManager provideCacheManager(
            @AuthCacheRegistry CacheRegistry cacheRegistry) {
        return new GuavaCacheManager(cacheRegistry);
    }

    @Provides
    @Singleton
    PermissionManager providePermissionManager(@Named("dao") PermissionManager permissionManager,
                                               InvalidatableCacheManager cacheManager,
                                               final PermissionResolver permissionResolver) {
        ImmutableMap.Builder<String, Set<Permission>> defaultRolePermissions = ImmutableMap.builder();

        Function<String, Permission> toPermission = new Function<String, Permission>() {
            @Override
            public Permission apply(String permissionString) {
                return permissionResolver.resolvePermission(permissionString);
            }
        };

        for (DefaultRoles defaultRole : DefaultRoles.values()) {
            Set<Permission> rolePermissions = FluentIterable.from(defaultRole.getPermissions())
                    .transform(toPermission)
                    .toSet();

            defaultRolePermissions.put(defaultRole.toString(), rolePermissions);
        }

        PermissionManager deferring = new DeferringPermissionManager(permissionManager, defaultRolePermissions.build());

        return new CacheManagingPermissionManager(deferring, cacheManager);
    }
}
