package com.bazaarvoice.emodb.auth.apikey;

import com.bazaarvoice.emodb.auth.EmoSecurityManager;
import com.google.common.collect.ImmutableList;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.realm.Realm;
import org.apache.shiro.subject.SubjectContext;

import java.util.List;

public class ApiKeySecurityManager extends DefaultSecurityManager implements EmoSecurityManager {

    public ApiKeySecurityManager(ApiKeyRealm realm) {
        super(ImmutableList.<Realm>of(realm));
    }

    /**
     * Required to by-pass a Shiro issue caused by not binding the SecurityManager globally.
     */
    @Override
    protected SubjectContext createSubjectContext() {
        SubjectContext subjectContext = super.createSubjectContext();
        subjectContext.setSecurityManager(this);
        return subjectContext;
    }

    private ApiKeyRealm getRealm() {
        // We explicitly set "realms" to a List in the constructor so the following unchecked cast is valid.
        return (ApiKeyRealm) ((List<Realm>) getRealms()).get(0);
    }

    @Override
    public boolean hasPermissionByInternalId(String internalId, String permission) {
        return getRealm().hasPermissionByInternalId(internalId, permission);
    }

    @Override
    public boolean hasPermissionByInternalId(String internalId, Permission permission) {
        return getRealm().hasPermissionByInternalId(internalId, permission);
    }

    @Override
    public boolean hasPermissionsByInternalId(String internalId, String... permissions) {
        return getRealm().hasPermissionsByInternalId(internalId, permissions);
    }

    @Override
    public boolean hasPermissionsByInternalId(String internalId, Permission... permissions) {
        return getRealm().hasPermissionsByInternalId(internalId, permissions);
    }
}
