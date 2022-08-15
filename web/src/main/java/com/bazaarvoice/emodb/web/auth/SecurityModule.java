package com.bazaarvoice.emodb.web.auth;

import com.bazaarvoice.emodb.auth.AuthCacheRegistry;
import com.bazaarvoice.emodb.auth.AuthZooKeeper;
import com.bazaarvoice.emodb.auth.EmoSecurityManager;
import com.bazaarvoice.emodb.auth.InternalAuthorizer;
import com.bazaarvoice.emodb.auth.SecurityManagerBuilder;
import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.dropwizard.DropwizardAuthConfigurator;
import com.bazaarvoice.emodb.auth.identity.AuthIdentityManager;
import com.bazaarvoice.emodb.auth.identity.AuthIdentityReader;
import com.bazaarvoice.emodb.auth.identity.CacheManagingAuthIdentityManager;
import com.bazaarvoice.emodb.auth.identity.DataCenterSynchronizedAuthIdentityManager;
import com.bazaarvoice.emodb.auth.identity.DeferringAuthIdentityManager;
import com.bazaarvoice.emodb.auth.identity.TableAuthIdentityManagerDAO;
import com.bazaarvoice.emodb.auth.permissions.CacheManagingPermissionManager;
import com.bazaarvoice.emodb.auth.permissions.DeferringPermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionIDs;
import com.bazaarvoice.emodb.auth.permissions.PermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionReader;
import com.bazaarvoice.emodb.auth.permissions.TablePermissionManagerDAO;
import com.bazaarvoice.emodb.auth.role.DataCenterSynchronizedRoleManager;
import com.bazaarvoice.emodb.auth.role.DeferringRoleManager;
import com.bazaarvoice.emodb.auth.role.Role;
import com.bazaarvoice.emodb.auth.role.RoleManager;
import com.bazaarvoice.emodb.auth.role.TableRoleManagerDAO;
import com.bazaarvoice.emodb.auth.shiro.GuavaCacheManager;
import com.bazaarvoice.emodb.auth.shiro.InvalidatableCacheManager;
import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.common.dropwizard.discovery.PayloadBuilder;
import com.bazaarvoice.emodb.common.dropwizard.guice.ServerCluster;
import com.bazaarvoice.emodb.common.dropwizard.guice.SystemTablePlacement;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.databus.ReplicationKey;
import com.bazaarvoice.emodb.databus.SystemIdentity;
import com.bazaarvoice.emodb.datacenter.DataCenterConfiguration;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.client.DataStoreClient;
import com.bazaarvoice.emodb.sor.compactioncontrol.CompControlApiKey;
import com.bazaarvoice.emodb.uac.api.AuthUserAccessControl;
import com.bazaarvoice.emodb.uac.client.UserAccessControlClientFactory;
import com.bazaarvoice.emodb.web.uac.LocalSubjectUserAccessControl;
import com.bazaarvoice.emodb.web.uac.ReadWriteDelegatingSubjectUserAccessControl;
import com.bazaarvoice.emodb.web.uac.RemoteSubjectUserAccessControl;
import com.bazaarvoice.emodb.web.uac.SubjectUserAccessControl;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.ServiceEndPointBuilder;
import com.bazaarvoice.ostrich.ServiceFactory;
import com.bazaarvoice.ostrich.discovery.FixedHostDiscovery;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.bazaarvoice.ostrich.retry.ExponentialBackoffRetry;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Longs;
import com.google.inject.Exposed;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import com.sun.jersey.api.client.Client;
import org.apache.curator.framework.CuratorFramework;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.PermissionResolver;
import org.apache.shiro.mgt.SecurityManager;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
 * <li> @{@link com.bazaarvoice.emodb.auth.AuthZooKeeper} {@link CuratorFramework}
 * </ul>
 * Exports the following:
 * <ul>
 * <li> {@link DropwizardAuthConfigurator}
 * <li> @{@link ReplicationKey} String
 * <li> @{@link SystemIdentity} String
 * <li> @{@link CompControlApiKey} String
 * <li> @{@link SystemIdentity} String
 * <li> {@link PermissionResolver}
 * <li> {@link InternalAuthorizer}
 * <li> {@link SubjectUserAccessControl}
 * </ul>
 */
public class SecurityModule extends PrivateModule {

    private final static String REALM_NAME = "EmoDB";
    private final static String ANONYMOUS_KEY = "anonymous";

    // Internal identifiers for reserved API keys
    private final static String ADMIN_ID = "__admin";
    private final static String REPLICATION_ID = "__replication";
    private final static String ANONYMOUS_ID = "__anonymous";

    // Internal identifier for reserved internal processes that do not have a public facing API key
    private final static String SYSTEM_INTERNAL_ID = "__system";

    @Override
    protected void configure() {
        bind(HashFunction.class).annotatedWith(ApiKeyHashFunction.class).toInstance(Hashing.sha256());
        bind(ApiKeyEncryption.class).asEagerSingleton();
        bind(RebuildMissingRolesTask.class).asEagerSingleton();

        bind(LocalSubjectUserAccessControl.class).asEagerSingleton();
        
        bind(new TypeLiteral<Set<String>>() {})
                .annotatedWith(ReservedRoles.class)
                .toInstance(ImmutableSet.of(
                        DefaultRoles.replication.toString()));

        bind(PermissionResolver.class).to(EmoPermissionResolver.class).asEagerSingleton();
        bind(SecurityManager.class).to(EmoSecurityManager.class);
        bind(InternalAuthorizer.class).to(EmoSecurityManager.class);
        bind(new TypeLiteral<AuthIdentityReader<ApiKey>>() {}).to(new TypeLiteral<AuthIdentityManager<ApiKey>>() {});
        bind(PermissionReader.class).to(PermissionManager.class);

        bind(String.class).annotatedWith(SystemIdentity.class).toInstance(SYSTEM_INTERNAL_ID);

        expose(DropwizardAuthConfigurator.class);
        expose(Key.get(String.class, ReplicationKey.class));
        expose(Key.get(String.class, CompControlApiKey.class));
        expose(Key.get(String.class, SystemIdentity.class));
        expose(PermissionResolver.class);
        expose(InternalAuthorizer.class);
        expose(SubjectUserAccessControl.class);
    }

    @Provides
    @Singleton
    @Inject
    EmoSecurityManager provideSecurityManager(
            AuthIdentityReader<ApiKey> authIdentityReader,
            PermissionReader permissionReader,
            InvalidatableCacheManager cacheManager,
            @Named("AnonymousKey") Optional<String> anonymousKey) {

        return SecurityManagerBuilder.create()
                .withRealmName(REALM_NAME)
                .withAuthIdentityReader(authIdentityReader)
                .withPermissionReader(permissionReader)
                .withAnonymousAccessAs(anonymousKey.orElse(null))
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
    @Named("AnonymousRoles")
    Set<String> provideAnonymousRoles(AuthorizationConfiguration config) {
        return config.getAnonymousRoles();
    }

    @Provides
    @Singleton
    @Named("AnonymousKey")
    Optional<String> provideAnonymousKey(@Named("AnonymousRoles") Set<String> anonymousRoles) {
        if (!anonymousRoles.isEmpty()) {
            return Optional.of(ANONYMOUS_KEY);
        }
        return Optional.empty();
    }

    /**
     * Supplier for generating uniquey IDs for API keys.  Note that, critically, the values returned will never
     * collide with the reserved IDs from {@link #provideAuthIdentityManagerWithDefaults(String, String, Optional, Set, AuthIdentityManager)}
     */
    @Provides
    @Singleton
    @IdentityIdSupplier
    Supplier<String> provideIdentityIdSupplier() {
        return () -> {
            // This is effectively a TimeUUID but condensed to a slightly smaller String representation.
            UUID uuid = TimeUUIDs.newUUID();
            byte[] b = new byte[16];
            System.arraycopy(Longs.toByteArray(uuid.getMostSignificantBits()), 0, b, 0, 8);
            System.arraycopy(Longs.toByteArray(uuid.getLeastSignificantBits()), 0, b, 8, 8);
            return BaseEncoding.base32().omitPadding().encode(b);
        };
    }

    @Provides
    @Singleton
    @Named("dao")
    AuthIdentityManager<ApiKey> provideAuthIdentityManagerDAO(
            AuthorizationConfiguration config, DataStore dataStore, @ApiKeyHashFunction HashFunction hash,
            @IdentityIdSupplier Supplier<String> identityIdSupplier, @SystemTablePlacement String tablePlacement) {
        return new TableAuthIdentityManagerDAO<>(ApiKey.class, dataStore, config.getIdentityTable(),
                config.getIdIndexTable(), tablePlacement, identityIdSupplier, hash);
    }

    @Provides
    @Singleton
    @Named("withDefaults")
    AuthIdentityManager<ApiKey> provideAuthIdentityManagerWithDefaults(
            @ReplicationKey String replicationKey,
            @Named("AdminKey") String adminKey, @Named("AnonymousKey") Optional<String> anonymousKey,
            @Named("AnonymousRoles") Set<String> anonymousRoles,
            @Named("dao") AuthIdentityManager<ApiKey> daoManager) {

        ImmutableMap.Builder<String, ApiKey> reservedIdentities = ImmutableMap.builder();
        reservedIdentities.put(replicationKey,
                createReservedApiKey(REPLICATION_ID, "replication", ImmutableSet.of(DefaultRoles.replication.toString())));
        reservedIdentities.put(adminKey,
                createReservedApiKey(ADMIN_ID, "admin", ImmutableSet.of(DefaultRoles.admin.toString())));

        if (anonymousKey.isPresent()) {
            reservedIdentities.put(anonymousKey.get(),
                    createReservedApiKey(ANONYMOUS_ID, "anonymous", anonymousRoles));
        }

        return new DeferringAuthIdentityManager<>(daoManager, reservedIdentities.build());
    }

    private ApiKey createReservedApiKey(String id, String description, Set<String> roles) {
        ApiKey apiKey = new ApiKey(id, roles);
        apiKey.setOwner("emodb");
        apiKey.setDescription(description);
        apiKey.setIssued(new Date(1471898640000L));
        apiKey.setMaskedId(Strings.repeat("*", 48));
        return apiKey;
    }

    @Provides
    @Singleton
    @Named("cacheInvalidating")
    AuthIdentityManager<ApiKey> provideAuthIdentityManagerCacheInvalidating(
            @Named("withDefaults") AuthIdentityManager<ApiKey> defaultedManager,
            InvalidatableCacheManager cacheManager) {
        return new CacheManagingAuthIdentityManager<>(defaultedManager, cacheManager);
    }

    @Provides
    @Singleton
    AuthIdentityManager<ApiKey> provideAuthIdentityManager(
            @Named("cacheInvalidating") AuthIdentityManager<ApiKey> cacheInvalidatingManager,
            @AuthZooKeeper CuratorFramework curator) {
        return new DataCenterSynchronizedAuthIdentityManager<>(cacheInvalidatingManager, curator);
    }

    @Provides
    @Singleton
    @Named("dao")
    PermissionManager providePermissionManagerDAO(
            AuthorizationConfiguration config, PermissionResolver permissionResolver, DataStore dataStore,
            @SystemTablePlacement String tablePlacement) {
        return new TablePermissionManagerDAO(
                permissionResolver, dataStore, config.getPermissionsTable(), tablePlacement);
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

        for (DefaultRoles defaultRole : DefaultRoles.values()) {
            Set<Permission> rolePermissions = defaultRole.getPermissions()
                    .stream()
                    .map(permissionResolver::resolvePermission)
                    .collect(Collectors.toSet());

            defaultRolePermissions.put(PermissionIDs.forRole(defaultRole.toString()), rolePermissions);
        }

        PermissionManager deferring = new DeferringPermissionManager(permissionManager, defaultRolePermissions.build());

        return new CacheManagingPermissionManager(deferring, cacheManager);
    }

    @Provides
    @Singleton
    @Named("dao")
    RoleManager provideRoleManagerDAO(AuthorizationConfiguration config, DataStore dataStore,
                                      PermissionManager permissionManager, @SystemTablePlacement String tablePlacement) {
        return new TableRoleManagerDAO(dataStore, config.getRoleTable(), config.getRoleGroupTable(),
                tablePlacement, permissionManager);
    }

    @Provides
    @Singleton
    @Named("withDefaults")
    RoleManager provideRoleManagerWithDefaultRoles(@Named("dao") RoleManager delegate) {
        List<Role> defaultRoles = Lists.newArrayList();
        for (DefaultRoles defaultRole : DefaultRoles.values()) {
            // Use the default role's name as both the role's identifier and name attribute
            defaultRoles.add(new Role(null, defaultRole.name(), defaultRole.name(),"Reserved role"));
        }
        return new DeferringRoleManager(delegate, defaultRoles);
    }

    @Provides
    @Singleton
    RoleManager provideRoleManager(@Named("withDefaults") RoleManager delegate, @AuthZooKeeper CuratorFramework curator) {
        return new DataCenterSynchronizedRoleManager(delegate, curator);
    }

    @Provides
    @Singleton
    SubjectUserAccessControl provideSubjectUserAccessControl(LocalSubjectUserAccessControl local,
                                                             DataCenterConfiguration dataCenterConfiguration,
                                                             @ServerCluster String cluster,
                                                             Client jerseyClient, MetricRegistry metricRegistry) {
        // If this is the system data center all user access control can be performed locally
        if (dataCenterConfiguration.isSystemDataCenter()) {
            return local;
        }

        // Create a client for forwarding user access control requests to the system data center
        ServiceFactory<AuthUserAccessControl> clientFactory = UserAccessControlClientFactory
                .forClusterAndHttpClient(cluster, jerseyClient);

        URI uri = dataCenterConfiguration.getSystemDataCenterServiceUri();
        ServiceEndPoint endPoint = new ServiceEndPointBuilder()
                .withServiceName(clientFactory.getServiceName())
                .withId(dataCenterConfiguration.getSystemDataCenter())
                .withPayload(new PayloadBuilder()
                        .withUrl(uri.resolve(DataStoreClient.SERVICE_PATH))
                        .withAdminUrl(uri)
                        .toString())
                .build();

        AuthUserAccessControl uac = ServicePoolBuilder.create(AuthUserAccessControl.class)
                .withMetricRegistry(metricRegistry)
                .withHostDiscovery(new FixedHostDiscovery(endPoint))
                .withServiceFactory(clientFactory)
                .buildProxy(new ExponentialBackoffRetry(30, 1, 10, TimeUnit.SECONDS));

        RemoteSubjectUserAccessControl remote = new RemoteSubjectUserAccessControl(uac);

        // Provide an instance which satisfies read requests locally and forwards write requests to the
        // system data center
        return new ReadWriteDelegatingSubjectUserAccessControl(local, remote);
    }
}
