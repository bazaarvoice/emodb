package com.bazaarvoice.emodb.auth.apikey;

import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.subject.SubjectContext;

public class ApiKeySecurityManager extends DefaultSecurityManager {

    public ApiKeySecurityManager(ApiKeyRealm realm) {
        super(realm);
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
}
