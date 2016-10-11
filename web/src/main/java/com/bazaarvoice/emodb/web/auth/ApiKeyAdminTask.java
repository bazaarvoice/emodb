package com.bazaarvoice.emodb.web.auth;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyAuthenticationToken;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.bazaarvoice.emodb.auth.identity.AuthIdentityManager;
import com.bazaarvoice.emodb.auth.identity.IdentityState;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.common.json.ISO8601DateFormat;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.google.common.net.HostAndPort;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import io.dropwizard.servlets.tasks.Task;
import org.apache.cassandra.thrift.AuthorizationException;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.subject.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.security.SecureRandom;
import java.util.Date;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

/**
 * Task for managing API keys and the roles associated with them.
 *
 * The following examples demonstrate the various ways to use this task.  In order to actually run this task you must
 * provide an API key which has permission to manage API keys (see {@link Permissions#manageApiKeys()}).  For
 * the purposes of this example the API key "admin-key" is a valid key with this permission.
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
 * The following example copies all metadata and associated roles from "sample-key" to a new key, removing
 * the ability for "sample-key" to be authorized or authenticated.
 *
 * <code>
 *     $ curl -XPOST "localhost:8081/tasks/api-key?action=migrate&APIKey=admin-key&key=sample-key"
 * </code>
 *
 * Inactivate API key
 * ==============
 *
 * The following example inactivates API key "sample-key".
 *
 * <code>
 *     $ curl -XPOST "localhost:8081/tasks/api-key?action=inactivate&APIKey=admin-key&key=sample-key"
 * </code>
 */
public class ApiKeyAdminTask extends Task {

    private final Logger _log = LoggerFactory.getLogger(ApiKeyAdminTask.class);

    private enum Action {
        CREATE,
        VIEW,
        UPDATE,
        MIGRATE,
        INACTIVATE,
        DELETE
    }

    private final SecurityManager _securityManager;
    private final AuthIdentityManager<ApiKey> _authIdentityManager;
    private final Set<String> _reservedRoles;
    private final SecureRandom _secureRandom;

    @Inject
    public ApiKeyAdminTask(SecurityManager securityManager,
                           TaskRegistry taskRegistry,
                           AuthIdentityManager<ApiKey> authIdentityManager,
                           @SelfHostAndPort HostAndPort selfHostAndPort,
                           @ReservedRoles Set<String> reservedRoles) {
        super("api-key");

        _securityManager = securityManager;
        _authIdentityManager = authIdentityManager;
        _reservedRoles = reservedRoles;

        // Create a randomizer that makes the odds of creating the same API key globally effectively zero
        _secureRandom = new SecureRandom();
        _secureRandom.setSeed(System.currentTimeMillis());
        _secureRandom.setSeed(Thread.currentThread().getId());
        _secureRandom.setSeed(selfHostAndPort.getHostText().getBytes());
        _secureRandom.setSeed(selfHostAndPort.getPort());

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

            // Make sure the API key is permitted to manage API keys
            subject.checkPermission(Permissions.manageApiKeys());

            String activityStr = getValueFromParams("action", parameters);
            Action action = Action.valueOf(activityStr.toUpperCase());

            switch (action) {
                case CREATE:
                    createApiKey(parameters, output);
                    break;
                case VIEW:
                    viewApiKey(parameters, output);
                    break;
                case UPDATE:
                    updateApiKey(parameters, output);
                    break;
                case MIGRATE:
                    migrateApiKey(parameters, output);
                    break;
                case INACTIVATE:
                    inactivateApiKey(parameters, output);
                    break;
                case DELETE:
                    deleteApiKey(parameters, output);
                    break;
            }
        } catch (AuthenticationException | AuthorizationException e) {
            _log.warn("Unauthorized attempt to access API key management task");
            output.println("Not authorized");
        } finally {
            subject.logout();
        }
    }

    private String createUniqueInternalId() {
        // This is effectively a TimeUUID but condensed to a slightly smaller String representation.
        UUID uuid = TimeUUIDs.newUUID();
        byte[] b = new byte[16];
        System.arraycopy(Longs.toByteArray(uuid.getMostSignificantBits()), 0, b, 0, 8);
        System.arraycopy(Longs.toByteArray(uuid.getLeastSignificantBits()), 0, b, 8, 8);
        return BaseEncoding.base32().omitPadding().encode(b);
    }

    /**
     * When creating or modifying a key the caller can provide an explicit key.  This isn't common and should be
     * restricted to integration tests where stable keys are desirable.  In production it is better
     * to let the system create random keys.
     */
    private String getUniqueOrProvidedKey(ImmutableMultimap<String, String> parameters, String explicitKeyParam, PrintWriter output) {
        String key = Iterables.getOnlyElement(parameters.get(explicitKeyParam), null);

        if (key != null) {
            if (!isProvidedApiKeyValid(key)) {
                output.println("Error:  Provided key is not valid");
                return null;
            }
            if (isApiKeyInUse(key)) {
                output.println("Error:  Provided key exists");
                return null;
            }
        } else {
            key = createUniqueApiKey();
        }

        return key;
    }

    private void createApiKey(ImmutableMultimap<String, String> parameters, PrintWriter output)
                throws Exception {
        String owner = getValueFromParams("owner", parameters);
        Set<String> roles = ImmutableSet.copyOf(parameters.get("role"));
        String description = Iterables.getFirst(parameters.get("description"), null);

        checkArgument(Sets.intersection(roles, _reservedRoles).isEmpty(), "Cannot assign reserved role");

        // Generate a unique internal ID for this new key
        String internalId = createUniqueInternalId();

        String key = getUniqueOrProvidedKey(parameters, "key", output);
        if (key == null) {
            return;
        }

        createApiKey(key, internalId, owner, roles, description);

        output.println("API key: " + key);
        output.println("\nWarning:  This is your only chance to see this key.  Save it somewhere now.");
    }

    private void createApiKey(String key, String internalId, String owner, Set<String> roles, String description) {
        ApiKey apiKey = new ApiKey(key, internalId, IdentityState.ACTIVE, roles);
        apiKey.setOwner(owner);
        apiKey.setDescription(description);
        apiKey.setIssued(new Date());

        _authIdentityManager.updateIdentity(apiKey);
    }

    private boolean isApiKeyInUse(String key) {
        return _authIdentityManager.getIdentity(key) != null;
    }

    private String createUniqueApiKey() {
        int attempt = 0;

        while (attempt++ < 10) {
            String key = generateRandomApiKey();
            if (!isApiKeyInUse(key)) {
                return key;
            }
        }

        // Instead of trying indefinitely, raise an exception if no unique API key was generated after 10 attempts
        throw new RuntimeException("Failed to generate unique API key after 10 attempts");
    }

    private synchronized String generateRandomApiKey() {

        // Use base64 encoding but keep the keys alphanumeric (we could use base64URL() to make them at least URL-safe
        // but pure alphanumeric keeps validation simple).

        byte[] rawKey = new byte[36];
        String key = "";
        do {
            _secureRandom.nextBytes(rawKey);
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

    private void viewApiKey(ImmutableMultimap<String, String> parameters, PrintWriter output)
            throws Exception {
        String key = getValueFromParams("key", parameters);

        ApiKey apiKey = _authIdentityManager.getIdentity(key);
        if (apiKey == null) {
            output.println("Unknown key");
        } else {
            output.println("owner: " + apiKey.getOwner());
            output.println("description: " + apiKey.getDescription());
            output.println("state: " + apiKey.getState());
            output.println("roles: " + Joiner.on(", ").join(apiKey.getRoles()));
            output.println("issued: " + ISO8601DateFormat.getInstance().format(apiKey.getIssued()));
        }
    }

    private void updateApiKey(ImmutableMultimap<String, String> parameters, PrintWriter output) {
        Set<String> addRoles = ImmutableSet.copyOf(parameters.get("addRole"));
        Set<String> removeRoles = ImmutableSet.copyOf(parameters.get("removeRole"));

        checkArgument(!addRoles.isEmpty() || !removeRoles.isEmpty(), "Update requires one or more 'addRole' or 'removeRole' parameters");
        checkArgument(Sets.intersection(addRoles, _reservedRoles).isEmpty(), "Cannot assign reserved role");

        String key = getValueFromParams("key", parameters);
        ApiKey apiKey = _authIdentityManager.getIdentity(key);
        checkArgument(apiKey != null, "Unknown API key");

        //noinspection ConstantConditions
        Set<String> roles = Sets.newHashSet(apiKey.getRoles());
        roles.addAll(addRoles);
        roles.removeAll(removeRoles);

        if (!roles.equals(apiKey.getRoles())) {
            ApiKey updatedKey = new ApiKey(key, apiKey.getInternalId(), apiKey.getState(), roles);
            updatedKey.setOwner(apiKey.getOwner());
            updatedKey.setDescription(apiKey.getDescription());
            updatedKey.setIssued(new Date());

            _authIdentityManager.updateIdentity(updatedKey);
        }

        output.println("API key updated");
    }

    private void migrateApiKey(ImmutableMultimap<String, String> parameters, PrintWriter output) {
        String key = getValueFromParams("key", parameters);
        checkArgument(isApiKeyInUse(key), "Unknown API key");

        String newKey = getUniqueOrProvidedKey(parameters, "newKey", output);
        if (newKey == null) {
            return;
        }

        _authIdentityManager.migrateIdentity(key, newKey);

        output.println("Migrated API key: " + newKey);
        output.println("\nWarning:  This is your only chance to see this key.  Save it somewhere now.");
    }

    private void inactivateApiKey(ImmutableMultimap<String, String> parameters, PrintWriter output) {
        String key = getValueFromParams("key", parameters);
        ApiKey apiKey = _authIdentityManager.getIdentity(key);
        checkArgument(apiKey != null, "Unknown API key");
        checkArgument(apiKey.getState().isActive(), "Cannot inactivate API key in state %s", apiKey.getState());
        apiKey.setState(IdentityState.INACTIVE);
        _authIdentityManager.updateIdentity(apiKey);
        output.println("API key inactivated");
    }

    private void deleteApiKey(ImmutableMultimap<String, String> parameters, PrintWriter output) {
        String key = getValueFromParams("key", parameters);

        // Normally it is not safe to delete an API key.  Doing so can open vectors for recreating the key or having a
        // new key whose hash collides with the deleted key.  However, for unit testing purposes there is a need to
        // actually delete the key from the system.  Historically the "delete" action was used for what is now
        // "inactivate".  To prevent accidental deletion and protect against users using the older "delete" action an
        // extra confirmation parameter is required.

        boolean confirmed = parameters.get("confirm").stream().anyMatch("true"::equalsIgnoreCase);

        if (!confirmed) {
            output.println("Deleting an API key is a potentially unsafe operation that should only be performed in limited circumstances.");
            output.println("If the intent is to make this API key unusable call this task again with the 'inactivate' action.");
            output.println("To confirm permanently deleting this API key call this task again with a 'confirm=true' parameter");
        } else {
            _authIdentityManager.deleteIdentityUnsafe(key);
            output.println("API key deleted");
        }
    }

    private String getValueFromParams(String value, ImmutableMultimap<String, String> parameters) {
        try {
            return Iterables.getOnlyElement(parameters.get(value));
        } catch (Exception e) {
            throw new IllegalArgumentException(format("A single '%s' parameter value is required", value));
        }
    }
}
