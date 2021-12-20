package com.bazaarvoice.emodb.web.uac;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyModification;
import com.bazaarvoice.emodb.auth.identity.AuthIdentityManager;
import com.bazaarvoice.emodb.auth.identity.AuthIdentityModification;
import com.bazaarvoice.emodb.auth.identity.IdentityExistsException;
import com.bazaarvoice.emodb.auth.identity.IdentityNotFoundException;
import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.bazaarvoice.emodb.auth.role.Role;
import com.bazaarvoice.emodb.auth.role.RoleExistsException;
import com.bazaarvoice.emodb.auth.role.RoleIdentifier;
import com.bazaarvoice.emodb.auth.role.RoleManager;
import com.bazaarvoice.emodb.auth.role.RoleModification;
import com.bazaarvoice.emodb.auth.role.RoleNotFoundException;
import com.bazaarvoice.emodb.common.api.ServiceUnavailableException;
import com.bazaarvoice.emodb.common.api.UnauthorizedException;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.uac.api.CreateEmoApiKeyRequest;
import com.bazaarvoice.emodb.uac.api.CreateEmoApiKeyResponse;
import com.bazaarvoice.emodb.uac.api.CreateEmoRoleRequest;
import com.bazaarvoice.emodb.uac.api.EmoApiKey;
import com.bazaarvoice.emodb.uac.api.EmoApiKeyExistsException;
import com.bazaarvoice.emodb.uac.api.EmoApiKeyNotFoundException;
import com.bazaarvoice.emodb.uac.api.EmoRole;
import com.bazaarvoice.emodb.uac.api.EmoRoleExistsException;
import com.bazaarvoice.emodb.uac.api.EmoRoleKey;
import com.bazaarvoice.emodb.uac.api.EmoRoleNotFoundException;
import com.bazaarvoice.emodb.uac.api.InsufficientRolePermissionException;
import com.bazaarvoice.emodb.uac.api.InvalidEmoPermissionException;
import com.bazaarvoice.emodb.uac.api.MigrateEmoApiKeyRequest;
import com.bazaarvoice.emodb.uac.api.UpdateEmoApiKeyRequest;
import com.bazaarvoice.emodb.uac.api.UpdateEmoRoleRequest;
import com.bazaarvoice.emodb.web.auth.EmoPermission;
import com.bazaarvoice.emodb.web.auth.Permissions;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.InvalidPermissionStringException;
import org.apache.shiro.authz.permission.PermissionResolver;

import java.security.SecureRandom;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.Spliterators;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * SubjectUserAccessControl implementation for servicing UAC requests locally.  It is expected that if a method requires
 * global consistency that it has already been forwarded to the correct data center, which is the local one.
 */
public class LocalSubjectUserAccessControl implements SubjectUserAccessControl {

    private final RoleManager _roleManager;
    private final PermissionResolver _permissionResolver;
    private final AuthIdentityManager<ApiKey> _authIdentityManager;
    private final HostAndPort _hostAndPort;
    private final Meter _lockTimeoutMeter;

    @Inject
    public LocalSubjectUserAccessControl(RoleManager roleManager, PermissionResolver permissionResolver,
                                         AuthIdentityManager<ApiKey> authIdentityManager,
                                         @SelfHostAndPort HostAndPort selfHostAndPort, MetricRegistry metricRegistry) {
        _roleManager = roleManager;
        _permissionResolver = permissionResolver;
        _authIdentityManager = authIdentityManager;
        _hostAndPort = selfHostAndPort;
        _lockTimeoutMeter = metricRegistry.meter(MetricRegistry.name("bv.emodb.web.uac", "acquire-update-lock", "timeouts"));
    }

    @Override
    public Iterator<EmoRole> getAllRoles(Subject subject) {
        // If it's not possible for the subject to view _any_ roles then raise unauthorized exception
        verifyPermission(subject, Permissions.readSomeRole());

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(_roleManager.getAll(), 0), false)
                .filter(role -> subject.hasPermission(Permissions.readRole(role.getRoleIdentifier())))
                .map(role -> convert(role, _roleManager.getPermissionsForRole(role.getRoleIdentifier())))
                .iterator();
    }

    @Override
    public Iterator<EmoRole> getAllRolesInGroup(Subject subject, String group) {
        String convertedGroup = convertUACGroup(group);

        // If it's not possible for the subject to view _any_ roles in the group then raise unauthorized exception
        verifyPermission(subject, Permissions.readSomeRoleInGroup(Permissions.toRoleGroupResource(convertedGroup)));

        return _roleManager.getRolesByGroup(convertedGroup).stream()
                .filter(role -> subject.hasPermission(Permissions.readRole(role.getRoleIdentifier())))
                .map(role -> convert(role, _roleManager.getPermissionsForRole(role.getRoleIdentifier())))
                .iterator();
    }

    @Override
    public EmoRole getRole(Subject subject, EmoRoleKey roleKey) {
        RoleIdentifier roleId = convert(roleKey);
        // Every API key has permission to view any of its assigned roles
        if (!subject.hasRole(roleId.toString())) {
            verifyPermission(subject, Permissions.readRole(roleId));
        }
        Role role = _roleManager.getRole(roleId);
        if (role == null) {
            // Unlike the client counterpart raise the following exception instead of returning null.  This results
            // in Jersey returning the appropriate NOT_FOUND response.
            throw new EmoRoleNotFoundException(roleKey.getGroup(), roleKey.getId());
        }
        return convert(role, _roleManager.getPermissionsForRole(roleId));
    }

    @Override
    public void createRole(Subject subject, CreateEmoRoleRequest request) {
        EmoRoleKey roleKey = request.getRoleKey();
        RoleIdentifier roleId = convert(roleKey);
        verifyPermission(subject, Permissions.createRole(roleId));
        verifyAllPermissionsAssignable(request.getPermissions());
        verifyAllowedToGrantOrRevokePermissions(subject, request.getPermissions(), true);

        RoleModification modification = new RoleModification()
                .withName(request.getName())
                .withDescription(request.getDescription())
                .withPermissionUpdate(new PermissionUpdateRequest()
                        .permit(request.getPermissions())
                        .revokeRest());
        try {
            _roleManager.createRole(roleId, modification);
        } catch (RoleExistsException e) {
            // Convert to API exception
            throw new EmoRoleExistsException(roleKey.getGroup(), roleKey.getId());
        } catch (InvalidPermissionStringException e) {
            // Convert to API exception
            throw new InvalidEmoPermissionException(e.getMessage(), e.getPermissionString());
        } catch (Exception e) {
            throw convertUncheckedException(e);
        }
    }

    @Override
    public void updateRole(Subject subject, UpdateEmoRoleRequest request) {
        EmoRoleKey roleKey = request.getRoleKey();
        RoleIdentifier roleId = convert(roleKey);
        verifyPermission(subject, Permissions.updateRole(roleId));
        verifyAllPermissionsAssignable(request.getGrantedPermissions());
        verifyAllPermissionsAssignable(request.getRevokedPermissions());
        verifyAllowedToGrantOrRevokePermissions(subject, request.getGrantedPermissions(), true);
        verifyAllowedToGrantOrRevokePermissions(subject, request.getRevokedPermissions(), false);

        RoleModification modification = new RoleModification();
        if (request.isNamePresent()) {
            modification.withName(request.getName());
        }
        if (request.isDescriptionPresent()) {
            modification.withDescription(request.getDescription());
        }

        PermissionUpdateRequest permissionUpdateRequest = new PermissionUpdateRequest();
        if (!request.getGrantedPermissions().isEmpty()) {
            permissionUpdateRequest.permit(request.getGrantedPermissions());
        }
        if (!request.getRevokedPermissions().isEmpty()) {
            permissionUpdateRequest.revoke(request.getRevokedPermissions());
        }
        if (request.isRevokeOtherPermissions()) {
            permissionUpdateRequest.revokeRest();
        }
        modification.withPermissionUpdate(permissionUpdateRequest);

        try {
            _roleManager.updateRole(roleId, modification);
        } catch (RoleNotFoundException e) {
            // Convert to API exception
            throw new EmoRoleNotFoundException(roleKey.getGroup(), roleKey.getId());
        } catch (InvalidPermissionStringException e) {
            // Convert to API exception
            throw new InvalidEmoPermissionException(e.getMessage(), e.getPermissionString());
        } catch (Exception e) {
            throw convertUncheckedException(e);
        }
    }

    @Override
    public void deleteRole(Subject subject, EmoRoleKey roleKey) {
        RoleIdentifier convertedId = convert(roleKey);
        verifyPermission(subject, Permissions.deleteRole(convertedId));

        // Backend doesn't care if the role exists or not, but could be confusing to the caller if delete returns
        // success for a role that doesn't exist.  Therefore check if it exists first.
        if (_roleManager.getRole(convertedId) == null) {
            throw new EmoRoleNotFoundException(roleKey.getGroup(), roleKey.getId());
        }

        // Caller cannot delete a role unless he has permission to revoke every permission granted to the role
        verifyAllowedToGrantOrRevokePermissions(subject, _roleManager.getPermissionsForRole(convertedId), false);

        try {
            _roleManager.deleteRole(convertedId);
        } catch (Exception e) {
            throw convertUncheckedException(e);
        }
    }

    private void verifyAllPermissionsAssignable(Iterable<String> permissions) {
        // Check each permission to ensure it is assignable
        for (String permission : permissions) {
            if (!resolvePermission(permission).isAssignable()) {
                throw new InvalidEmoPermissionException("Permission cannot be assigned", permission);
            }
        }
    }

    private void verifyAllowedToGrantOrRevokePermissions(Subject subject, Iterable<String> permissions, boolean isGrant) {
        // Check each permission to ensure the permission is within the subject's own permissions
        for (String permission : permissions) {
            if (!subject.hasPermission(resolvePermission(permission))) {
                if (isGrant) {
                    throw new InsufficientRolePermissionException("Insufficient permission to grant permission to role: " + permission, permission);
                } else {
                    throw new InsufficientRolePermissionException("Insufficient permission to revoke permission from role: " + permission, permission);
                }
            }
        }
    }

    private EmoPermission resolvePermission(String permission) {
        try {
            // All permissions returned by the resolver are EmoPermissions
            return (EmoPermission) _permissionResolver.resolvePermission(permission);
        } catch (InvalidPermissionStringException e) {
            // Permission string was invalid; convert to the API exception
            throw new InvalidEmoPermissionException(e.getMessage(), e.getPermissionString());
        }
    }

    @Override
    public boolean checkRoleHasPermission(Subject subject, EmoRoleKey roleKey, String permission) {
        RoleIdentifier convertedId = convert(roleKey);
        // Permission for this action is tied to the ability to read the role
        verifyPermission(subject, Permissions.readRole(convertedId));

        return checkRoleHasPermission(convertedId, resolvePermission(permission), true);
    }

    private boolean checkRoleHasPermission(RoleIdentifier roleId, Permission permission, boolean raiseRoleNotFoundException) {
        Set<String> rolePermissions = _roleManager.getPermissionsForRole(roleId);
        if (rolePermissions.isEmpty()) {
            if (raiseRoleNotFoundException) {
                // Check whether the role exists and, if not, raise the appropriate exception
                if (_roleManager.getRole(roleId) == null) {
                    EmoRoleKey roleKey = convert(roleId);
                    throw new EmoRoleNotFoundException(roleKey.getGroup(), roleKey.getId());
                }
            }
            // Either the role exists but has no permissions or the role doesn't exist.  Either way return false
            return false;
        }


        for (String rolePermission : rolePermissions) {
            if (resolvePermission(rolePermission).implies(permission)) {
                // All it takes is one
                return true;
            }
        }
        return false;
    }

    //******************************************************************************************************************

    @Override
    public EmoApiKey getApiKey(Subject subject, String id) {
        // Every user has implicit permission to view herself
        if (!subject.getId().equals(id)) {
            verifyPermission(subject, Permissions.readApiKey());
        }
        ApiKey apiKey = _authIdentityManager.getIdentity(id);
        if (apiKey == null) {
            throw new EmoApiKeyNotFoundException();
        }
        return convert(apiKey);
    }

    @Override
    public EmoApiKey getApiKeyByKey(Subject subject, String key) {
        // The ability to look up an API key by key could be a security hole since a malicious user could use it
        // to brute-force test whether API keys exist.  For this reason this method requires a special permission
        // which should only be granted with extreme caution.  This is in addition to the general permission to
        // read API keys.
        verifyPermission(subject, Permissions.readApiKey());
        verifyPermission(subject, Permissions.findApiKeyByKey());
        ApiKey apiKey = _authIdentityManager.getIdentityByAuthenticationId(key);
        if (apiKey == null) {
            throw new EmoApiKeyNotFoundException();
        }
        return convert(apiKey);
    }

    @Override
    public CreateEmoApiKeyResponse createApiKey(Subject subject, CreateEmoApiKeyRequest request) {
        verifyPermission(subject, Permissions.createApiKey());
        String exactKey = request.getCustomRequestParameters().get("key").stream().findFirst().orElse(null);

        if (exactKey != null) {
            // Typically the system creates a random API key for the caller, so the caller has no a-priori knowledge of what
            // the API key will be.  However, for integration tests it is often helpful to create an exact key provided by
            // the caller.  This creates a higher security risk and therefore requires a distinct permission from creating
            // random keys.
            verifyPermission(subject, Permissions.createExactApiKey());
        }

        Set<RoleIdentifier> roleIds = convert(request.getRoles());
        verifyPermissionToGrantRoles(subject, roleIds);

        Set<String> roleIdStrings = Sets.newLinkedHashSet();
        for (RoleIdentifier roleId : roleIds) {
            roleIdStrings.add(roleId.toString());
        }

        checkArgument(!Strings.isNullOrEmpty(request.getOwner()), "Non-empty owner is required");

        AuthIdentityModification<ApiKey> modification = new ApiKeyModification()
                .withOwner(request.getOwner())
                .withDescription(request.getDescription())
                .addRoles(roleIdStrings);

        CreateEmoApiKeyResponse response;
        if (exactKey != null) {
            // Ensure the key conforms to the same format as an auto-generated key.
            verifyProvidedKeyIsValid(exactKey);
            response = createExactApiKey(exactKey, modification);
        } else {
            response = createRandomApiKey(modification);
        }
        return response;
    }

    private void verifyProvidedKeyIsValid(String apiKey) {
        checkArgument(Pattern.matches("[a-zA-Z0-9]{48}", apiKey), "API key must be exactly 48 alpha-numeric characters");
    }

    private String generateRandomApiKey() {
        // Randomize the API key such that it is practically assured that no two call will create the same API key
        // at the same time.
        SecureRandom random = new SecureRandom();
        random.setSeed(System.currentTimeMillis());
        random.setSeed(Thread.currentThread().getId());
        random.setSeed(_hostAndPort.getHostText().getBytes());
        random.setSeed(_hostAndPort.getPort());

        // Use base64 encoding but keep the keys alphanumeric (we could use base64URL() to make them at least URL-safe
        // but pure alphanumeric keeps validation simple).

        byte[] rawKey = new byte[36];
        String key = "";
        do {
            random.nextBytes(rawKey);
            String chars = BaseEncoding.base64().omitPadding().encode(rawKey).toLowerCase();
            // Eliminate all '+' an '/' characters
            chars = chars.replaceAll("\\+|/", "");
            key += chars;
        } while (key.length() < 48);

        return key.substring(0, 48);
    }

    private CreateEmoApiKeyResponse createRandomApiKey(AuthIdentityModification<ApiKey> modification) {
        // Since the API key is randomly generated call create in a loop to ensure we don't grab one that is already picked

        String key = null;
        String internalId = null;
        int attempt = 0;

        while (key == null && ++attempt < 10) {
            key = generateRandomApiKey();
            try {
                internalId = _authIdentityManager.createIdentity(key, modification);
            } catch (IdentityExistsException e) {
                // API keys are randomly generated, so this should be exceptionally rare.  Try again with
                // a new randomly-generated key
                key = null;
            } catch (Exception e) {
                throw convertUncheckedException(e);
            }
        }

        if (key == null) {
            throw new ServiceUnavailableException("Failed to generate unique key", 1);
        }
        return new CreateEmoApiKeyResponse(key, internalId);
    }

    private CreateEmoApiKeyResponse createExactApiKey(String key, AuthIdentityModification<ApiKey> modification) {
        try {
            String internalId = _authIdentityManager.createIdentity(key, modification);
            return new CreateEmoApiKeyResponse(key, internalId);
        } catch (IdentityExistsException e) {
            // Convert to to API exception
            throw new EmoApiKeyExistsException();
        } catch (Exception e) {
            throw convertUncheckedException(e);
        }
    }

    @Override
    public void updateApiKey(Subject subject, UpdateEmoApiKeyRequest request) {
        String id = request.getId();
        Set<RoleIdentifier> addRoles = convert(request.getAssignedRoles());

        Set<RoleIdentifier> removeRoles = convert(request.getUnassignedRoles());
        if (request.isUnassignOtherRoles()) {
            // Need to explicitly fetch which roles will be removed and verify permissions.
            ApiKey apiKey = _authIdentityManager.getIdentity(id);
            // Don't throw an ApiKeyNotFoundException yet if it doesn't exist, failing for lack of permission takes precedence.
            if (apiKey != null) {
                for (String existingRoleString : apiKey.getRoles()) {
                    RoleIdentifier existingRoleId = RoleIdentifier.fromString(existingRoleString);
                    if (!addRoles.contains(existingRoleId)) {
                        removeRoles.add(existingRoleId);
                    }
                }
            }
        }

        // If any role attributes are being modified that requires API key update permission
        if (request.isOwnerPresent() || request.isDescriptionPresent()) {
            verifyPermission(subject, Permissions.updateApiKey());
        }

        verifyPermissionToGrantRoles(subject, Iterables.concat(addRoles, removeRoles));

        ApiKeyModification modification = new ApiKeyModification();
        if (request.isOwnerPresent()) {
            checkArgument(!Strings.isNullOrEmpty(request.getOwner()), "Owner cannot be null or empty");
            modification.withOwner(request.getOwner());
        }
        if (request.isDescriptionPresent()) {
            modification.withDescription(request.getDescription());
        }
        if (!addRoles.isEmpty()) {
            modification.addRoles(toStringSet(addRoles));
        }
        if (!removeRoles.isEmpty()) {
            modification.removeRoles(toStringSet(removeRoles));
        }

        try {
            _authIdentityManager.updateIdentity(id, modification);
        } catch (IdentityNotFoundException e) {
            // Convert to API exception
            throw new EmoApiKeyNotFoundException();
        } catch (Exception e) {
            throw convertUncheckedException(e);
        }
    }

    @Override
    public String migrateApiKey(Subject subject, MigrateEmoApiKeyRequest request) {
        // Migrating a key is considered a key update operation, so check for update permission
        verifyPermission(subject, Permissions.updateApiKey());

        String id = request.getId();
        String key = request.getCustomRequestParameters().get("key").stream().findFirst().orElse(null);

        if (key != null) {
            // As with create, verify the caller has permission to migrate to an exact key and that the key is valid
            verifyPermission(subject, Permissions.createExactApiKey());
            verifyProvidedKeyIsValid(key);

            try {
                _authIdentityManager.migrateIdentity(id, key);
            } catch (IdentityExistsException e) {
                throw new EmoApiKeyExistsException();
            } catch (Exception e) {
                throw convertUncheckedException(e);
            }
        } else {
            key = migrateToRandomApiKey(id);
        }

        return key;
    }

    private String migrateToRandomApiKey(String id) {
        // Since the API key is randomly generated call create in a loop to ensure we don't grab one that is already picked

        String key = null;
        int attempt = 0;

        while (key == null && ++attempt < 10) {
            key = generateRandomApiKey();
            try {
                _authIdentityManager.migrateIdentity(id, key);
            } catch (IdentityNotFoundException e) {
                throw new EmoApiKeyNotFoundException();
            } catch (IdentityExistsException e) {
                // API keys are randomly generated, so this should be exceptionally rare.  Try again with
                // a new randomly-generated key
                key = null;
            } catch (Exception e) {
                throw convertUncheckedException(e);
            }
        }

        if (key == null) {
            throw new ServiceUnavailableException("Failed to generate unique key", 1);
        }
        return key;
    }

    @Override
    public void deleteApiKey(Subject subject, String id) {
        verifyPermission(subject, Permissions.deleteApiKey());

        ApiKey apiKey = _authIdentityManager.getIdentity(id);
        if (apiKey == null) {
            throw new EmoApiKeyNotFoundException();
        }

        // Does the caller have permission to remove every role from the API key?  Note that there is a small race
        // condition here since it's possible another synchronous call adds a new role to the key between this check
        // and the following deleteIdentity() call.  However, this risk should be low and, even if it were to happen,
        // the end result is technically still serializable.

        Set<RoleIdentifier> roleIds = Sets.newLinkedHashSet();
        for (String roleString : apiKey.getRoles()) {
            roleIds.add(RoleIdentifier.fromString(roleString));
        }

        verifyPermissionToGrantRoles(subject, roleIds);
        
        try {
            _authIdentityManager.deleteIdentity(id);
        } catch (IdentityNotFoundException e) {
            // Convert to API exception
            throw new EmoApiKeyNotFoundException();
        } catch (Exception e) {
            throw convertUncheckedException(e);
        }
    }

    @Override
    public boolean checkApiKeyHasPermission(Subject subject, String id, String permission) {
        // Permission for this action is tied to the ability to read the key
        if (!subject.getId().equals(id)) {
            verifyPermission(subject, Permissions.readApiKey());
        }
        ApiKey apiKey = _authIdentityManager.getIdentity(id);
        if (apiKey == null) {
            throw new EmoApiKeyNotFoundException();
        }

        Permission resolvedPermission = resolvePermission(permission);
        for (String role : apiKey.getRoles()) {
            // We don't care if the API key has a non-existent role assigned, so don't raise an exception, just
            // move on to the next role.
            if (checkRoleHasPermission(RoleIdentifier.fromString(role), resolvedPermission, false)) {
                // All it takes is one
                return true;
            }
        }

        return false;
    }

    /**
     * Verifies whether the user has a specific permission.  If not it throws a standard UnauthorizedException.
     * @throws UnauthorizedException User did not have the permission
     */
    private void verifyPermission(Subject subject, String permission)
            throws UnauthorizedException {
        if (!subject.hasPermission(permission)) {
            throw new UnauthorizedException();
        }
    }

    /**
     * Verifies whether the user has permission to grant all of the provided roles.  If not it throws an UnauthorizedException.
     * @throws UnauthorizedException User did not have the permission to grant at least one role.
     */
    private void verifyPermissionToGrantRoles(Subject subject, Iterable<RoleIdentifier> roleIds) {
        Set<RoleIdentifier> unauthorizedIds = Sets.newTreeSet();
        boolean anyAuthorized = false;

        for (RoleIdentifier roleId : roleIds) {
            // Verify the caller has permission to grant this role
            if (subject.hasPermission(Permissions.grantRole(roleId))) {
                anyAuthorized = true;
            } else {
                unauthorizedIds.add(roleId);
            }
        }

        if (!unauthorizedIds.isEmpty()) {
            // If the caller was not authorized to assign any of the provided roles raise a generic exception, otherwise
            // the exception provides insight as to which roles were unauthorized.  This provides some helpful feedback
            // where appropriate without exposing potentially exploitable information about whether the subject's API key
            // is valid.
            if (!anyAuthorized) {
                throw new UnauthorizedException();
            } else {
                throw new UnauthorizedException("Not authorized for roles: " + Joiner.on(", ").join(unauthorizedIds));
            }
        }
    }

    /**
     * Converts unchecked exceptions to appropriate API exceptions.  Specifically, if the subsystem fails to acquire
     * the synchronization lock for a non-read operation it will throw a TimeoutException.  This method converts
     * that to a ServiceUnavailableException.  All other exceptions are rethrown as-is.
     *
     * This method never returns, it always results in an exception being thrown.  The return value is present to
     * support more natural exception handling by the caller.
     */
    private RuntimeException convertUncheckedException(Exception e) {
        if (Throwables.getRootCause(e) instanceof TimeoutException) {
            _lockTimeoutMeter.mark();
            throw new ServiceUnavailableException("Failed to acquire update lock, try again later", new Random().nextInt(5) + 1);
        }
        Throwables.propagateIfPossible(e);
        throw new RuntimeException(e);
    }

    // The following methods are helpful converters for transforming UAC and Auth representations of roles and keys.

    private static EmoRoleKey convert(RoleIdentifier role) {
        return new EmoRoleKey(role.getGroup(), role.getId());
    }

    private static RoleIdentifier convert(EmoRoleKey roleKey) {
        return new RoleIdentifier(convertUACGroup(roleKey.getGroup()), roleKey.getId());
    }

    private static Set<RoleIdentifier> convert(Set<EmoRoleKey> roles) {
        return roles.stream().map(LocalSubjectUserAccessControl::convert).collect(Collectors.toCollection(Sets::newHashSet));
    }

    private static EmoRole convert(Role role, Set<String> permissions) {
        EmoRole converted = new EmoRole(new EmoRoleKey(role.getGroup(), role.getId()));
        converted.setName(role.getName());
        converted.setDescription(role.getDescription());
        converted.setPermissions(permissions);
        return converted;
    }

    private static String convertUACGroup(String group) {
        return EmoRoleKey.NO_GROUP.equals(group) ? null : group;
    }

    private static EmoApiKey convert(ApiKey apiKey) {
        EmoApiKey converted = new EmoApiKey(apiKey.getId(), apiKey.getMaskedId(), apiKey.getIssued());
        converted.setOwner(apiKey.getOwner());
        converted.setDescription(apiKey.getDescription());
        converted.setRoles(
                apiKey.getRoles().stream()
                        .map(RoleIdentifier::fromString)
                        .map(LocalSubjectUserAccessControl::convert)
                        .collect(Collectors.toSet()));
        return converted;
    }

    private Set<String> toStringSet(Set<RoleIdentifier> roleIds) {
        return roleIds.stream().map(Object::toString).collect(Collectors.toCollection(Sets::newHashSet));
    }
}
