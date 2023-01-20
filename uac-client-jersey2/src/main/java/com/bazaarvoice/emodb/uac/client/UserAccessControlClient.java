package com.bazaarvoice.emodb.uac.client;

import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.client.EmoClientException;
import com.bazaarvoice.emodb.client.EmoResource;
import com.bazaarvoice.emodb.client.EmoResponse;
import com.bazaarvoice.emodb.client.uri.EmoUriBuilder;
import com.bazaarvoice.emodb.common.api.ServiceUnavailableException;
import com.bazaarvoice.emodb.common.api.UnauthorizedException;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.common.json.JsonStreamProcessingException;
import com.bazaarvoice.emodb.uac.api.AuthUserAccessControl;
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
import com.fasterxml.jackson.core.type.TypeReference;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;


import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;


import static java.util.Objects.requireNonNull;

/**
 * Client implementation of {@link AuthUserAccessControl} which makes REST calls to the Emo service.
 */
public class UserAccessControlClient implements AuthUserAccessControl {

    private static final MediaType APPLICATION_X_CREATE_ROLE_TYPE = new MediaType("application", "x.json-create-role");
    private static final MediaType APPLICATION_X_UPDATE_ROLE_TYPE = new MediaType("application", "x.json-update-role");
    private static final MediaType APPLICATION_X_CREATE_API_KEY_TYPE = new MediaType("application", "x.json-create-api-key");
    private static final MediaType APPLICATION_X_UPDATE_API_KEY_TYPE = new MediaType("application", "x.json-update-api-key");

    private final EmoClient _client;
    private final UriBuilder _uac;
    private final RetryPolicy<Object> _retryPolicy;
    
    public UserAccessControlClient(URI endPoint, EmoClient client, RetryPolicy<Object> retryPolicy) {
        _client = requireNonNull(client, "client");
        _uac = EmoUriBuilder.fromUri(endPoint);
        _retryPolicy = requireNonNull(retryPolicy);
    }

    @Override
    public Iterator<EmoRole> getAllRoles(String apiKey) {
        URI uri = _uac.clone()
                .segment("role")
                .build();
        return Failsafe.with(_retryPolicy)
                .get(() -> _client.resource(uri)
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .get(new TypeReference<Iterator<EmoRole>>() {
                        }));
    }

    @Override
    public Iterator<EmoRole> getAllRolesInGroup(String apiKey, String group) {

        URI uri = _uac.clone()
                .segment("role")
                .segment(Optional.ofNullable(group).orElse(EmoRoleKey.NO_GROUP))
                .build();
        return Failsafe.with(_retryPolicy)
                .get(() -> _client.resource(uri)
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .get(new TypeReference<Iterator<EmoRole>>() {
                        }));
    }

    @Override
    public EmoRole getRole(String apiKey, EmoRoleKey roleKey) {
        requireNonNull(roleKey, "roleKey");
        URI uri = _uac.clone()
                .segment("role")
                .segment(roleKey.getGroup())
                .segment(roleKey.getId())
                .build();
        EmoResponse response = Failsafe.with(_retryPolicy)
                .get(() -> _client.resource(uri)
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .get(EmoResponse.class));

        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return response.getEntity(EmoRole.class);
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            return null;
        }

        throw new EmoClientException(response);
    }

    @Override
    public void createRole(String apiKey, CreateEmoRoleRequest request)
            throws EmoRoleExistsException {
        requireNonNull(request, "request");
        EmoRoleKey roleKey = requireNonNull(request.getRoleKey(), "roleKey");

        URI uri = _uac.clone()
                .segment("role")
                .segment(roleKey.getGroup())
                .segment(roleKey.getId())
                .build();
        Failsafe.with(_retryPolicy)
                .run(() -> _client.resource(uri)
                        .type(APPLICATION_X_CREATE_ROLE_TYPE)
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .post(JsonHelper.asJson(request)));

    }

    @Override
    public void updateRole(String apiKey, UpdateEmoRoleRequest request)
            throws EmoRoleNotFoundException {
        requireNonNull(request, "request");
        EmoRoleKey roleKey = requireNonNull(request.getRoleKey(), "roleKey");
        URI uri = _uac.clone()
                .segment("role")
                .segment(roleKey.getGroup())
                .segment(roleKey.getId())
                .build();
        Failsafe.with(_retryPolicy)
                .run(() -> _client.resource(uri)
                        .type(APPLICATION_X_UPDATE_ROLE_TYPE)
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .put(JsonHelper.asJson(request)));
    }

    @Override
    public void deleteRole(String apiKey, EmoRoleKey roleKey)
            throws EmoRoleNotFoundException {
        requireNonNull(roleKey, "roleKey");

        URI uri = _uac.clone()
                .segment("role")
                .segment(roleKey.getGroup())
                .segment(roleKey.getId())
                .build();
        Failsafe.with(_retryPolicy)
                .run(() -> _client.resource(uri)
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .delete());
    }

    @Override
    public boolean checkRoleHasPermission(String apiKey, EmoRoleKey roleKey, String permission)
            throws EmoRoleNotFoundException {
        requireNonNull(roleKey, "roleKey");
        requireNonNull(permission, "permission");
        URI uri = _uac.clone()
                .segment("role")
                .segment(roleKey.getGroup())
                .segment(roleKey.getId())
                .segment("permitted")
                .queryParam("permission", permission)
                .build();
        return Failsafe.with(_retryPolicy)
                .get(() -> _client.resource(uri)
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .get(Boolean.class));
    }

    @Override
    public EmoApiKey getApiKey(String apiKey, String id) {
        requireNonNull(id, "id");
        URI uri = _uac.clone()
                .segment("api-key")
                .segment(id)
                .build();
        EmoResponse response = Failsafe.with(_retryPolicy)
                .get(() -> _client.resource(uri)
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .get(EmoResponse.class));
        return getApiKeyFromResponse(response);
    }

    @Override
    public EmoApiKey getApiKeyByKey(String apiKey, String key) {
        requireNonNull(key, "key");
        URI uri = _uac.clone()
                .segment("api-key")
                .segment("_key")
                .segment(key)
                .build();
        EmoResponse response = Failsafe.with(_retryPolicy)
                .get(() -> _client.resource(uri)
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .get(EmoResponse.class));
        return getApiKeyFromResponse(response);
    }

    private EmoApiKey getApiKeyFromResponse(EmoResponse response) {
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return response.getEntity(EmoApiKey.class);
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            return null;
        }

        throw (new EmoClientException(response));
    }

    @Override
    public CreateEmoApiKeyResponse createApiKey(String apiKey, CreateEmoApiKeyRequest request)
            throws EmoApiKeyNotFoundException {
        requireNonNull(request, "request");
        if (isBlankString(request.getOwner())) {
            throw new IllegalArgumentException("Non-empty owner is required");
        }

        URI uri = _uac.clone()
                .segment("api-key")
                .build();
        EmoResource resource = _client.resource(uri);

        for (Map.Entry<String, String> customQueryParam : request.getCustomRequestParameters().entries()) {
            resource = resource.queryParam(customQueryParam.getKey(), customQueryParam.getValue());
        }
        EmoResource finalResource = resource;
        return Failsafe.with(_retryPolicy)
                .get(() -> finalResource
                        .type(APPLICATION_X_CREATE_API_KEY_TYPE)
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .post(CreateEmoApiKeyResponse.class, JsonHelper.asJson(request)));

    }

    @Override
    public void updateApiKey(String apiKey, UpdateEmoApiKeyRequest request)
            throws EmoApiKeyNotFoundException {
        requireNonNull(request, "request");
        String id = requireNonNull(request.getId(), "id");
        requireNonNull(request.getOwner(), "owner is required");
        if (isBlankString(request.getOwner()) || !request.isOwnerPresent()) {
            throw new IllegalArgumentException("Non-empty owner is required");
        }

        URI uri = _uac.clone()
                .segment("api-key")
                .segment(id)
                .build();
        Failsafe.with(_retryPolicy)
                .run(() -> _client.resource(uri)
                        .type(APPLICATION_X_UPDATE_API_KEY_TYPE)
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .put(JsonHelper.asJson(request)));
    }

    @Override
    public String migrateApiKey(String apiKey, String id)
            throws EmoApiKeyNotFoundException {
        return migrateApiKey(apiKey, new MigrateEmoApiKeyRequest(id));
    }

    @Override
    public String migrateApiKey(String apiKey, MigrateEmoApiKeyRequest request) {
        requireNonNull(request, "request");
        String id = requireNonNull(request.getId(), "id");

        URI uri = _uac.clone()
                .segment("api-key")
                .segment(id)
                .segment("migrate")
                .build();

        EmoResource resource = _client.resource(uri);

        for (Map.Entry<String, String> customQueryParam : request.getCustomRequestParameters().entries()) {
            resource = resource.queryParam(customQueryParam.getKey(), customQueryParam.getValue());
        }

        EmoResource finalResource = resource;
        CreateEmoApiKeyResponse response = Failsafe.with(_retryPolicy)
                .get(() -> finalResource
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .post(CreateEmoApiKeyResponse.class, null));

        return response.getKey();
    }

    @Override
    public void deleteApiKey(String apiKey, String id)
            throws EmoApiKeyNotFoundException {
        requireNonNull(id, "id");
        URI uri = _uac.clone()
                .segment("api-key")
                .segment(id)
                .build();
        Failsafe.with(_retryPolicy)
                .run(() -> _client.resource(uri)
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .delete());
    }

    @Override
    public boolean checkApiKeyHasPermission(String apiKey, String id, String permission)
            throws EmoApiKeyNotFoundException {
        requireNonNull(id, "id");
        requireNonNull(permission, "permission");
        URI uri = _uac.clone()
                .segment("api-key")
                .segment(id)
                .segment("permitted")
                .queryParam("permission", permission)
                .build();
        return Failsafe.with(_retryPolicy)
                .get(() -> _client.resource(uri)
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .get(Boolean.class));
    }



    private boolean isBlankString(String string) {
        return string == null || string.trim().isEmpty();
    }
}
