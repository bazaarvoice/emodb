package com.bazaarvoice.emodb.web.auth;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyAuthenticationToken;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyModification;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.bazaarvoice.emodb.auth.identity.AuthIdentityManager;
import com.bazaarvoice.emodb.auth.identity.IdentityExistsException;
import com.bazaarvoice.emodb.auth.identity.IdentityNotFoundException;
import com.bazaarvoice.emodb.auth.role.RoleIdentifier;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.common.json.ISO8601DateFormat;
import com.bazaarvoice.emodb.web.auth.resource.VerifiableResource;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import io.dropwizard.servlets.tasks.Task;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authz.AuthorizationException;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.subject.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

/**
 * Task for managing API keys and the roles associated with them.
 *
 * The following examples demonstrate the various ways to use this task.  In order to actually run this task you must
 * provide an API key which has permission to perform the action requested, such as {@link Permissions#updateApiKey()}.
 * Additionally, in order to grant or revoke roles for an API key you must provide an API key with grant permission
 * for those roles, such as {@link Permissions#grantRole(VerifiableResource, VerifiableResource)}.
 * For the purposes of this example the API key "admin-key" is a valid key with all required permissions.
 *
 * Create API key
 * ==============
 *
 * When creating an API key you can assign it one or more roles.  Each role determines what permissions the API key will
 * have.  For a more details on the available roles see {@link DefaultRoles}.
 *
 * The following example creates a new API key with standard record and databus access:
 *
 * <code>
 *     $ curl -XPOST "localhost:8081/tasks/api-key?action=create&APIKey=admin-key\
 *     &owner=ermacs-dev@bazaarvoice.com&description=Ermacs+application&role=record_standard&role=databus_standard"
 * </code>
 *
 * View API key
 * ============
 *
 * The following example displays the metadata and roles associated with key "sample-key".
 *
 * <code>
 *     $ curl -XPOST "localhost:8081/tasks/api-key?action=view&APIKey=admin-key&key=sample-key"
 * </code>
 *
 * Update API key
 * ==============
 *
 * The following example removes the "record_standard" role from key "sample-key" and replaces it with the more
 * restrictive "record_update" role.  Any other roles associated with "sample-key" remain unchanged.
 *
 * <code>
 *     $ curl -XPOST "localhost:8081/tasks/api-key?action=update&APIKey=admin-key&key=sample-key\
 *     &removeRole=record_standard&addRole=record_update"
 * </code>
 *
 *
 * Migrate API key
 * ===============
 *
 * The following example copies all metadata and associated roles from "sample-key" to a new key then
 * deletes "sample-key".
 *
 * <code>
 *     $ curl -XPOST "localhost:8081/tasks/api-key?action=migrate&APIKey=admin-key&key=sample-key"
 * </code>
 *
 * Delete API key
 * ==============
 *
 * The following example deletes API key "sample-key".
 *
 * <code>
 *     $ curl -XPOST "localhost:8081/tasks/api-key?action=delete&APIKey=admin-key&key=sample-key"
 * </code>
 */
public class ApiKeyAdminTask extends Task {

    private final Logger _log = LoggerFactory.getLogger(ApiKeyAdminTask.class);

    private enum Action {
        CREATE,
        VIEW,
        UPDATE,
        MIGRATE,
        DELETE
    }

    private final SecurityManager _securityManager;
    private final AuthIdentityManager<ApiKey> _authIdentityManager;
    private final HostAndPort _hostAndPort;
    private final Set<String> _reservedRoles;

    @Inject
    public ApiKeyAdminTask(SecurityManager securityManager,
                           TaskRegistry taskRegistry,
                           AuthIdentityManager<ApiKey> authIdentityManager,
                           @SelfHostAndPort HostAndPort selfHostAndPort,
                           @ReservedRoles Set<String> reservedRoles) {
        super("api-key");

        _securityManager = securityManager;
        _authIdentityManager = authIdentityManager;
        _hostAndPort = selfHostAndPort;
        _reservedRoles = reservedRoles;

        taskRegistry.addTask(this);
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter output)
            throws Exception {
        Subject subject = new Subject.Builder(_securityManager).buildSubject();
        try {
            // Make sure the API key is valid
            String apiKey = getValueFromParams(ApiKeyRequest.AUTHENTICATION_PARAM, parameters);
            subject.login(new ApiKeyAuthenticationToken(apiKey));
            
            String activityStr = getValueFromParams("action", parameters);
            Action action = Action.valueOf(activityStr.toUpperCase());

            switch (action) {
                case CREATE:
                    createApiKey(subject, parameters, output);
                    break;
                case VIEW:
                    viewApiKey(subject, parameters, output);
                    break;
                case UPDATE:
                    updateApiKey(subject, parameters, output);
                    break;
                case MIGRATE:
                    migrateApiKey(subject, parameters, output);
                    break;
                case DELETE:
                    deleteApiKey(subject, parameters, output);
                    break;
            }
        } catch (AuthenticationException | AuthorizationException e) {
            _log.warn("Unauthorized attempt to access API key management task");
            output.println("Not authorized");
        } catch (IdentityNotFoundException e) {
            output.println("Unknown API key");
        } catch (Exception e) {
            if (Throwables.getRootCause(e) instanceof TimeoutException) {
                output.println("Timed out, try again later");
            } else {
                throw Throwables.propagate(e);
            }
        } finally {
            subject.logout();
        }
    }
    private void createApiKey(Subject subject, ImmutableMultimap<String, String> parameters, PrintWriter output)
                throws Exception {
        String owner = getValueFromParams("owner", parameters);
        Set<String> roles = ImmutableSet.copyOf(parameters.get("role"));
        String description = Iterables.getFirst(parameters.get("description"), null);

        // Does the caller have permission to create API keys?
        subject.checkPermission(Permissions.createApiKey());
        // Does the caller have permission to grant each role provided?
        for (String role : roles) {
            subject.checkPermission(Permissions.grantRole(RoleIdentifier.fromString(role)));
        }

        // If the caller provided a specific key then only use that one.  This isn't common and should be
        // restricted to integration tests where stable keys are desirable.  In production it is better
        // to let the system create random keys.
        Optional<String> providedKey = parameters.get("key").stream().findFirst();

        checkArgument(Sets.intersection(roles, _reservedRoles).isEmpty(), "Cannot assign reserved role");

        String id;
        String key;
        if (providedKey.isPresent()) {
            key = providedKey.get();
            if (!isProvidedApiKeyValid(key)) {
                output.println("Error:  Provided key is not valid");
                return;
            }
            if ((id = createApiKeyIfAvailable(key, owner, roles, description)) == null) {
                output.println("Error:  Provided key exists");
                return;
            }
        } else {
            String[] result = createRandomApiKey(owner, roles, description);
            id = result[0];
            key = result[1];
        }

        output.println("API key: " + key);
        output.println("ID: " + id);
        output.println("\nWarning:  This is your only chance to see this key.  Save it somewhere now.");
    }

    private String createApiKeyIfAvailable(String key,String owner, Set<String> roles, String description) {
        try {
            return _authIdentityManager.createIdentity(key,
                    new ApiKeyModification()
                            .withOwner(owner)
                            .withDescription(description)
                            .addRoles(roles));
        } catch (IdentityExistsException e) {
            return null;
        }
    }

    // Returns a String array of length two containing: { id, key }
    private String[] createRandomApiKey(String owner, Set<String> roles, String description) {
        // Since API keys are stored hashed we create them in a loop to ensure we don't grab one that is already picked

        String key = null;
        String id = null;

        while (id == null) {
            key = generateRandomApiKey();
            id = createApiKeyIfAvailable(key, owner, roles, description);
        }

        return new String[] { id, key };
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

    private boolean isProvidedApiKeyValid(String apiKey) {
        return Pattern.matches("[a-zA-Z0-9]{48}", apiKey);
    }

    private void viewApiKey(Subject subject, ImmutableMultimap<String, String> parameters, PrintWriter output)
            throws Exception {
        subject.checkPermission(Permissions.readApiKey());
        ApiKey apiKey = null;

        try {
            String id = getIdFromParams(parameters);
            apiKey = _authIdentityManager.getIdentity(id);
        } catch (IdentityNotFoundException e) {
            // Ok, api key will be null, so fall through
        }
        
        if (apiKey == null) {
            output.println("Unknown key");
        } else {
            output.println("owner: " + apiKey.getOwner());
            output.println("description: " + apiKey.getDescription());
            output.println("roles: " + Joiner.on(", ").join(apiKey.getRoles()));
            output.println("issued: " + ISO8601DateFormat.getInstance().format(apiKey.getIssued()));
        }
    }

    private void updateApiKey(Subject subject, ImmutableMultimap<String, String> parameters, PrintWriter output) {
        Set<String> addRoles = ImmutableSet.copyOf(parameters.get("addRole"));
        Set<String> removeRoles = ImmutableSet.copyOf(parameters.get("removeRole"));

        checkArgument(!addRoles.isEmpty() || !removeRoles.isEmpty(), "Update requires one or more 'addRole' or 'removeRole' parameters");
        checkArgument(Sets.intersection(addRoles, _reservedRoles).isEmpty(), "Cannot assign reserved role");

        // Does the caller have permission to grant all roles being added or removed?
        for (String role : addRoles) {
            subject.checkPermission(Permissions.grantRole(RoleIdentifier.fromString(role)));
        }
        for (String role : removeRoles) {
            subject.checkPermission(Permissions.grantRole(RoleIdentifier.fromString(role)));
        }

        String id = getIdFromParams(parameters);
        ApiKey apiKey = _authIdentityManager.getIdentity(id);
        checkArgument(apiKey != null, "Unknown API key");

        _authIdentityManager.updateIdentity(id,
                new ApiKeyModification()
                        .addRoles(addRoles)
                        .removeRoles(removeRoles));
        output.println("API key updated");
    }

    private void migrateApiKey(Subject subject, ImmutableMultimap<String, String> parameters, PrintWriter output) {
        // Migrating a key is considered a key update operation, so check for update permission
        subject.checkPermission(Permissions.updateApiKey());

        String id = getIdFromParams(parameters);
        String newKey = generateRandomApiKey();
        _authIdentityManager.migrateIdentity(id, newKey);
        output.println("Migrated API key: " + newKey);
        output.println("\nWarning:  This is your only chance to see this key.  Save it somewhere now.");
    }

    private void deleteApiKey(Subject subject, ImmutableMultimap<String, String> parameters, PrintWriter output) {
        try {
            String id = getIdFromParams(parameters);
            ApiKey apiKey = _authIdentityManager.getIdentity(id);
            if (apiKey != null) {
                // Does the caller have permission to revoke every role from the API key?
                for (String role : apiKey.getRoles()) {
                    subject.checkPermission(Permissions.grantRole(RoleIdentifier.fromString(role)));
                }

                _authIdentityManager.deleteIdentity(id);
            }
        } catch (IdentityNotFoundException e) {
            // Don't return an error to the caller if the API key already didn't exist.
        }
        output.println("API key deleted");
    }

    private String getIdFromParams(ImmutableMultimap<String, String> parameters) {
        // The current preferred method is for the caller to provide the API key's ID rather than they key itself,
        // which is supposed to be secret.  For continuity we'll accept both although IDs are preferred and there is
        // no guarantee key's will continue to be supported.
        Collection<String> ids = parameters.get("id");
        Collection<String> keys = parameters.get("key");

        checkArgument(ids.size() + keys.size() == 1, "Exactly one ID or key is required");

        String id;
        if (!ids.isEmpty()) {
            id = Iterables.getFirst(ids, null);
        } else {
            // Must convert key to ID
            ApiKey apiKey = _authIdentityManager.getIdentityByAuthenticationId(Iterables.getFirst(keys, null));
            if (apiKey != null) {
                id = apiKey.getId();
            } else {
                throw new IdentityNotFoundException();
            }
        }

        return id;
    }

    private String getValueFromParams(String value, ImmutableMultimap<String, String> parameters) {
        try {
            return Iterables.getOnlyElement(parameters.get(value));
        } catch (Exception e) {
            throw new IllegalArgumentException(format("A single '%s' parameter value is required", value));
        }
    }
}
