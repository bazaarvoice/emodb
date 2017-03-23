package com.bazaarvoice.emodb.uac.client;

import com.bazaarvoice.emodb.uac.api.AuthUserAccessControl;
import com.bazaarvoice.emodb.uac.api.CreateEmoApiKeyRequest;
import com.bazaarvoice.emodb.uac.api.CreateEmoApiKeyResponse;
import com.bazaarvoice.emodb.uac.api.CreateEmoRoleRequest;
import com.bazaarvoice.emodb.uac.api.EmoApiKey;
import com.bazaarvoice.emodb.uac.api.EmoApiKeyNotFoundException;
import com.bazaarvoice.emodb.uac.api.EmoRole;
import com.bazaarvoice.emodb.uac.api.EmoRoleExistsException;
import com.bazaarvoice.emodb.uac.api.EmoRoleKey;
import com.bazaarvoice.emodb.uac.api.EmoRoleNotFoundException;
import com.bazaarvoice.emodb.uac.api.MigrateEmoApiKeyRequest;
import com.bazaarvoice.emodb.uac.api.UpdateEmoApiKeyRequest;
import com.bazaarvoice.emodb.uac.api.UpdateEmoRoleRequest;
import com.bazaarvoice.emodb.uac.api.UserAccessControl;

import java.util.Iterator;

/**
 * Proxy implementation for delegating to a {@link AuthUserAccessControl} using a single, constant API key, and
 * exposing the delegate with a simpler {@link UserAccessControl} interface.
 */
public class UserAccessControlAuthenticatorProxy implements UserAccessControl {

    protected final AuthUserAccessControl _delegate;
    protected final String _apiKey;

    public UserAccessControlAuthenticatorProxy(AuthUserAccessControl delegate, String apiKey) {
        _delegate = delegate;
        _apiKey = apiKey;
    }

    @Override
    public Iterator<EmoRole> getAllRoles() {
        return _delegate.getAllRoles(_apiKey);
    }

    @Override
    public Iterator<EmoRole> getAllRolesInGroup(String group) {
        return _delegate.getAllRolesInGroup(_apiKey, group);
    }

    @Override
    public EmoRole getRole(EmoRoleKey roleKey) {
        return _delegate.getRole(_apiKey, roleKey);
    }

    @Override
    public void createRole(CreateEmoRoleRequest request) throws EmoRoleExistsException {
        _delegate.createRole(_apiKey, request);
    }

    @Override
    public void updateRole(UpdateEmoRoleRequest request) throws EmoRoleNotFoundException {
        _delegate.updateRole(_apiKey, request);
    }

    @Override
    public void deleteRole(EmoRoleKey roleKey) throws EmoRoleNotFoundException {
        _delegate.deleteRole(_apiKey, roleKey);
    }

    @Override
    public boolean checkRoleHasPermission(EmoRoleKey roleKey, String permission) throws EmoRoleNotFoundException {
        return _delegate.checkRoleHasPermission(_apiKey, roleKey, permission);
    }

    @Override
    public EmoApiKey getApiKey(String id) {
        return _delegate.getApiKey(_apiKey, id);
    }

    @Override
    public EmoApiKey getApiKeyByKey(String key) {
        return _delegate.getApiKeyByKey(_apiKey, key);
    }

    @Override
    public CreateEmoApiKeyResponse createApiKey(CreateEmoApiKeyRequest request) {
        return _delegate.createApiKey(_apiKey, request);
    }

    @Override
    public void updateApiKey(UpdateEmoApiKeyRequest request) throws EmoApiKeyNotFoundException {
        _delegate.updateApiKey(_apiKey, request);
    }

    @Override
    public String migrateApiKey(String id) throws EmoApiKeyNotFoundException {
        return _delegate.migrateApiKey(_apiKey, id);
    }

    @Override
    public String migrateApiKey(MigrateEmoApiKeyRequest request) throws EmoApiKeyNotFoundException {
        return _delegate.migrateApiKey(_apiKey, request);
    }

    @Override
    public void deleteApiKey(String id) throws EmoApiKeyNotFoundException {
        _delegate.deleteApiKey(_apiKey, id);
    }

    @Override
    public boolean checkApiKeyHasPermission(String id, String permission) throws EmoApiKeyNotFoundException {
        return _delegate.checkApiKeyHasPermission(_apiKey, id, permission);
    }
}
