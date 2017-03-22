package com.bazaarvoice.emodb.web.resources.uac;

import com.google.inject.Inject;
import org.apache.shiro.authz.annotation.RequiresAuthentication;

import javax.ws.rs.Path;

/**
 * User access control resources.  Thin top-level resource which forwards role and API key requests to the appropriate
 * resource.
 */
@Path("/uac/1")
@RequiresAuthentication
public class UserAccessControlResource1 {

    private final RoleResource1 _role;
    private final ApiKeyResource1 _apiKey;

    @Inject
    public UserAccessControlResource1(RoleResource1 role, ApiKeyResource1 apiKey) {
        _role = role;
        _apiKey = apiKey;
    }

    @Path("role")
    public RoleResource1 getRole() {
        return _role;
    }

    @Path("api-key")
    public ApiKeyResource1 getApiKey() {
        return _apiKey;
    }
}
