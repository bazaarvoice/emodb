package com.bazaarvoice.emodb.web.auth;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyAuthenticationToken;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.bazaarvoice.emodb.auth.identity.AuthIdentityManager;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.common.json.ISO8601DateFormat;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.google.common.net.HostAndPort;
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

    private void createApiKey(ImmutableMultimap<String, String> parameters, PrintWriter output)
                throws Exception {
        String owner = getValueFromParams("owner", parameters);
        Set<String> roles = ImmutableSet.copyOf(parameters.get("role"));
        String description = Iterables.getFirst(parameters.get("description"), null);

        // If the caller provided a specific key then only use that one.  This isn't common and should be
        // restricted to integration tests where stable keys are desirable.  In production it is better
        // to let the system create random keys.
        Optional<String> providedKey = Iterables.tryFind(parameters.get("key"), Predicates.alwaysTrue());

        checkArgument(Sets.intersection(roles, _reservedRoles).isEmpty(), "Cannot assign reserved role");

        String key;
        if (providedKey.isPresent()) {
            key = providedKey.get();
            if (!isProvidedApiKeyValid(key)) {
                output.println("Error:  Provided key is not valid");
                return;
            }
            if (!createApiKeyIfAvailable(key, owner, roles, description)) {
                output.println("Error:  Provided key exists");
                return;
            }
        } else {
            key = createRandomApiKey(owner, roles, description);
        }

        output.println("API key: " + key);
        output.println("\nWarning:  This is your only chance to see this key.  Save it somewhere now.");
    }

    private boolean createApiKeyIfAvailable(String key, String owner, Set<String> roles, String description) {
        boolean exists = _authIdentityManager.getIdentity(key) != null;

        if (exists) {
            return false;
        }

        ApiKey apiKey = new ApiKey(key, roles);
        apiKey.setOwner(owner);
        apiKey.setDescription(description);
        apiKey.setIssued(new Date());

        _authIdentityManager.updateIdentity(apiKey);

        return true;
    }

    private String createRandomApiKey(String owner, Set<String> roles, String description) {
        // Since API keys are stored hashed we create them in a loop to ensure we don't grab one that is already picked

        String key = null;
        boolean apiKeyCreated = false;

        while (!apiKeyCreated) {
            key = generateRandomApiKey();
            apiKeyCreated = createApiKeyIfAvailable(key, owner, roles, description);
        }

        return key;
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

    private void viewApiKey(ImmutableMultimap<String, String> parameters, PrintWriter output)
            throws Exception {
        String key = getValueFromParams("key", parameters);

        ApiKey apiKey = _authIdentityManager.getIdentity(key);
        if (apiKey == null) {
            output.println("Unknown key");
        } else {
            output.println("owner: " + apiKey.getOwner());
            output.println("description: " + apiKey.getDescription());
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
            ApiKey updatedKey = new ApiKey(key, roles);
            updatedKey.setOwner(apiKey.getOwner());
            updatedKey.setDescription(apiKey.getDescription());
            updatedKey.setIssued(new Date());

            _authIdentityManager.updateIdentity(updatedKey);
        }

        output.println("API key updated");
    }

    private void migrateApiKey(ImmutableMultimap<String, String> parameters, PrintWriter output) {
        String key = getValueFromParams("key", parameters);
        ApiKey apiKey = _authIdentityManager.getIdentity(key);
        checkArgument(apiKey != null, "Unknown API key");

        // Create a new key with the same information as the existing one
        //noinspection ConstantConditions
        String newKey = createRandomApiKey(apiKey.getOwner(), apiKey.getRoles(), apiKey.getDescription());
        // Delete the existing key
        _authIdentityManager.deleteIdentity(key);

        output.println("Migrated API key: " + newKey);
        output.println("\nWarning:  This is your only chance to see this key.  Save it somewhere now.");
    }

    private void deleteApiKey(ImmutableMultimap<String, String> parameters, PrintWriter output) {
        String key = getValueFromParams("key", parameters);
        _authIdentityManager.deleteIdentity(key);
        output.println("API key deleted");
    }

    private String getValueFromParams(String value, ImmutableMultimap<String, String> parameters) {
        try {
            return Iterables.getOnlyElement(parameters.get(value));
        } catch (Exception e) {
            throw new IllegalArgumentException(format("A single '%s' parameter value is required", value));
        }
    }
}
