package com.bazaarvoice.emodb.auth.jersey;

import com.bazaarvoice.emodb.auth.shiro.AnonymousToken;
import com.bazaarvoice.emodb.auth.shiro.PrincipalWithRoles;

import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;
import org.eclipse.jetty.security.UserAuthentication;
import org.eclipse.jetty.server.HttpConnection;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.UserIdentity;

import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.util.Objects;

/**
 * Resource filter for authentication.  This filter attempts to log the client in prior to handling each request.
 * Because the API is assumed to be RESTful this call also logs the user back out once the request is complete.
 * Internal caching performed by Shiro should make re-authentication of multiple calls from the same user efficient.
 */
public class AuthenticationResourceFilter implements ContainerRequestFilter, ContainerResponseFilter {

    private final SecurityManager _securityManager;
    private final AuthenticationTokenGenerator<?> _tokenGenerator;

    public AuthenticationResourceFilter(SecurityManager securityManager, AuthenticationTokenGenerator<?> tokenGenerator) {
        _securityManager = securityManager;
        _tokenGenerator = tokenGenerator;
    }



    @Override
    public void filter(ContainerRequestContext request) {
        Subject subject = new Subject.Builder(_securityManager).buildSubject();
        ThreadContext.bind(subject);

        AuthenticationToken token = _tokenGenerator.createToken(request);
        if (token == null) {
            token = AnonymousToken.getInstance();
        }
        subject.login(token);

        // The user has been successfully logged in.  Update the container authentication.
        setJettyAuthentication(subject);
    }

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) throws IOException {
        Subject subject = ThreadContext.getSubject();
        if (subject != null) {
            if (subject.isAuthenticated()) {
                subject.logout();
            }
            ThreadContext.unbindSubject();
        }
    }

    /**
     * Certain aspects of the container, such as logging, need the authentication information to behave properly.
     * This method updates the request with the necessary objects to recognize the authenticated user.
     */
    private void setJettyAuthentication(Subject subject) {
        // In unit test environments there may not be a current connection.  If any nulls are encountered
        // then, by definition, there is no container to update.
        HttpConnection connection = HttpConnection.getCurrentConnection();
        if (connection == null) {
            return;
        }
        Request jettyRequest = connection.getHttpChannel().getRequest();
        if (jettyRequest == null) {
            return;
        }

        // This cast down is safe; subject is always created with this type of principal
        PrincipalWithRoles principal = (PrincipalWithRoles) subject.getPrincipal();
        UserIdentity identity = principal.toUserIdentity();

        jettyRequest.setAuthentication(new UserAuthentication(SecurityContext.BASIC_AUTH, identity));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AuthenticationResourceFilter that = (AuthenticationResourceFilter) o;
        return _securityManager.equals(that._securityManager) && _tokenGenerator.equals(that._tokenGenerator);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_securityManager, _tokenGenerator);
    }
}
