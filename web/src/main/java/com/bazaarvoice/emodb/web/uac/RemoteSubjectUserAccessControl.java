package com.bazaarvoice.emodb.web.uac;

import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.uac.api.AuthUserAccessControl;
import com.bazaarvoice.emodb.uac.api.CreateEmoApiKeyRequest;
import com.bazaarvoice.emodb.uac.api.CreateEmoApiKeyResponse;
import com.bazaarvoice.emodb.uac.api.CreateEmoRoleRequest;
import com.bazaarvoice.emodb.uac.api.EmoApiKey;
import com.bazaarvoice.emodb.uac.api.EmoRole;
import com.bazaarvoice.emodb.uac.api.EmoRoleKey;
import com.bazaarvoice.emodb.uac.api.MigrateEmoApiKeyRequest;
import com.bazaarvoice.emodb.uac.api.UpdateEmoApiKeyRequest;
import com.bazaarvoice.emodb.uac.api.UpdateEmoRoleRequest;

import java.util.Arrays;
import java.util.Iterator;

import static java.util.Objects.requireNonNull;

/**
 * SubjectUserAccessControl implementation which forwards all requests to an {@link AuthUserAccessControl} using the
 * subject's API key.  Typically the AuthUserAccessControl implementation is a REST client to a remote data center.
 */
public class RemoteSubjectUserAccessControl implements SubjectUserAccessControl {

    private final AuthUserAccessControl _client;

    public RemoteSubjectUserAccessControl(AuthUserAccessControl client) {
        _client = requireNonNull(client, "client");
    }

    @Override
    public Iterator<EmoRole> getAllRoles(Subject subject) {
        return _client.getAllRoles(subject.getAuthenticationId());
    }

    @Override
    public Iterator<EmoRole> getAllRolesInGroup(Subject subject, String group) {
        return _client.getAllRolesInGroup(subject.getAuthenticationId(), group);
    }

    @Override
    public EmoRole getRole(Subject subject, EmoRoleKey roleKey) {
        return _client.getRole(subject.getAuthenticationId(), roleKey);
    }

    @Override
    public void createRole(Subject subject, CreateEmoRoleRequest request) {
        _client.createRole(subject.getAuthenticationId(), request);
    }

    @Override
    public void updateRole(Subject subject, UpdateEmoRoleRequest request) {
        _client.updateRole(subject.getAuthenticationId(), request);
    }

    @Override
    public void deleteRole(Subject subject, EmoRoleKey roleKey) {
        _client.deleteRole(subject.getAuthenticationId(), roleKey);
    }

    @Override
    public boolean checkRoleHasPermission(Subject subject, EmoRoleKey roleKey, String permission) {
        return _client.checkRoleHasPermission(subject.getAuthenticationId(), roleKey, permission);
    }

    @Override
    public EmoApiKey getApiKey(Subject subject, String id) {
        return _client.getApiKey(subject.getAuthenticationId(), id);
    }

    @Override
    public EmoApiKey getApiKeyByKey(Subject subject, String key) {
        return _client.getApiKeyByKey(subject.getAuthenticationId(), key);
    }

    @Override
    public CreateEmoApiKeyResponse createApiKey(Subject subject, CreateEmoApiKeyRequest request) {
        System.out.println("RemoteSubjectUserAccessControl.createApiKey() for roles: "
                +Arrays.asList(request.getRoles()));
        return _client.createApiKey(subject.getAuthenticationId(), request);
    }

    @Override
    public void updateApiKey(Subject subject, UpdateEmoApiKeyRequest request) {
        _client.updateApiKey(subject.getAuthenticationId(), request);
    }

    @Override
    public String migrateApiKey(Subject subject, MigrateEmoApiKeyRequest request) {
        return _client.migrateApiKey(subject.getAuthenticationId(), request);
    }

    @Override
    public void deleteApiKey(Subject subject, String id) {
        _client.deleteApiKey(subject.getAuthenticationId(), id);
    }

    @Override
    public boolean checkApiKeyHasPermission(Subject subject, String id, String permission) {
        return _client.checkApiKeyHasPermission(subject.getAuthenticationId(), id, permission);
    }
}
