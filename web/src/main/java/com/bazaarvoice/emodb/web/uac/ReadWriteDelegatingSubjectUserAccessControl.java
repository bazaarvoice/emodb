package com.bazaarvoice.emodb.web.uac;

import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.uac.api.CreateEmoApiKeyRequest;
import com.bazaarvoice.emodb.uac.api.CreateEmoApiKeyResponse;
import com.bazaarvoice.emodb.uac.api.CreateEmoRoleRequest;
import com.bazaarvoice.emodb.uac.api.EmoApiKey;
import com.bazaarvoice.emodb.uac.api.EmoRole;
import com.bazaarvoice.emodb.uac.api.EmoRoleKey;
import com.bazaarvoice.emodb.uac.api.MigrateEmoApiKeyRequest;
import com.bazaarvoice.emodb.uac.api.UpdateEmoApiKeyRequest;
import com.bazaarvoice.emodb.uac.api.UpdateEmoRoleRequest;

import java.util.Iterator;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of SubjectUserAccessControl which takes two delegates, a read and a write delegate.
 * All read operations are forwarded to the former and all operations which are potentially mutative, such as
 * creates, updates, and deletes, are forwarded to the latter.
 *
 * This class is injected as the SubjectUserAccessControl when the local data center is not the system data center,
 * since to ensure global consistency all writes must be performed in a single data center.  It allows for writes
 * to take place in the system data center without forcing a remote call for read operations.
 */
public class ReadWriteDelegatingSubjectUserAccessControl implements SubjectUserAccessControl {

    private final SubjectUserAccessControl _read;
    private final SubjectUserAccessControl _write;

    public ReadWriteDelegatingSubjectUserAccessControl(SubjectUserAccessControl read, SubjectUserAccessControl write) {
        _read = requireNonNull(read, "read");
        _write = requireNonNull(write, "write");
    }

    @Override
    public Iterator<EmoRole> getAllRoles(Subject subject) {
        return _read.getAllRoles(subject);
    }

    @Override
    public Iterator<EmoRole> getAllRolesInGroup(Subject subject, String group) {
        return _read.getAllRolesInGroup(subject, group);
    }

    @Override
    public EmoRole getRole(Subject subject, EmoRoleKey roleKey) {
        return _read.getRole(subject, roleKey);
    }

    @Override
    public void createRole(Subject subject, CreateEmoRoleRequest request) {
        _write.createRole(subject, request);
    }

    @Override
    public void updateRole(Subject subject, UpdateEmoRoleRequest request) {
        _write.updateRole(subject, request);
    }

    @Override
    public void deleteRole(Subject subject, EmoRoleKey roleKey) {
        _write.deleteRole(subject, roleKey);
    }

    @Override
    public boolean checkRoleHasPermission(Subject subject, EmoRoleKey roleKey, String permission) {
        return _read.checkRoleHasPermission(subject, roleKey, permission);
    }

    @Override
    public EmoApiKey getApiKey(Subject subject, String id) {
        return _read.getApiKey(subject, id);
    }

    @Override
    public EmoApiKey getApiKeyByKey(Subject subject, String key) {
        return _read.getApiKeyByKey(subject, key);
    }

    @Override
    public CreateEmoApiKeyResponse createApiKey(Subject subject, CreateEmoApiKeyRequest request) {
        return _write.createApiKey(subject, request);
    }

    @Override
    public void updateApiKey(Subject subject, UpdateEmoApiKeyRequest request) {
        _write.updateApiKey(subject, request);
    }

    @Override
    public String migrateApiKey(Subject subject, MigrateEmoApiKeyRequest request) {
        return _write.migrateApiKey(subject, request);
    }

    @Override
    public void deleteApiKey(Subject subject, String id) {
        _write.deleteApiKey(subject, id);
    }

    @Override
    public boolean checkApiKeyHasPermission(Subject subject, String id, String permission) {
        return _read.checkApiKeyHasPermission(subject, id, permission);
    }
}
