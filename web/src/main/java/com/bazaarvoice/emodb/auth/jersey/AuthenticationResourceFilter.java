package com.bazaarvoice.emodb.auth.jersey;

import com.bazaarvoice.emodb.auth.shiro.AnonymousToken;
import com.bazaarvoice.emodb.web.auth.shiro.PrincipalWithRoles;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;
import com.sun.jersey.spi.container.ContainerResponse;
import com.sun.jersey.spi.container.ContainerResponseFilter;
import com.sun.jersey.spi.container.ResourceFilter;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;
import org.eclipse.jetty.security.UserAuthentication;
import org.eclipse.jetty.server.HttpConnection;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.UserIdentity;

import javax.ws.rs.core.SecurityContext;

/**
 * Resource filter for authentication.  This filter attempts to log the client in prior to handling each request.
 * Because the API is assumed to be RESTful this call also logs the user back out once the request is complete.
 * Internal caching performed by Shiro should make re-authentication of multiple calls from the same user efficient.
 */
public class AuthenticationResourceFilter implements ResourceFilter, ContainerRequestFilter, ContainerResponseFilter {

    private final SecurityManager _securityManager;
    private final AuthenticationTokenGenerator<?> _tokenGenerator;

    public AuthenticationResourceFilter(SecurityManager securityManager, AuthenticationTokenGenerator<?> tokenGenerator) {
        _securityManager = securityManager;
        _tokenGenerator = tokenGenerator;
    }

    @Override
    public ContainerRequestFilter getRequestFilter() {
        return this;
    }

    @Override
    public ContainerResponseFilter getResponseFilter() {
        return this;
    }

    @Override
    public ContainerRequest filter(ContainerRequest request) {
        Subject subject = new Subject.Builder(_securityManager).buildSubject();
        ThreadContext.bind(subject);

        AuthenticationToken token = _tokenGenerator.createToken(request);
        if (token == null) {
            token = AnonymousToken.getInstance();
        }
        subject.login(token);

        // The user has been successfully logged in.  Update the container authentication.
        setJettyAuthentication(subject);

        return request;
    }

    @Override
    public ContainerResponse filter(ContainerRequest request, ContainerResponse response) {
        Subject subject = ThreadContext.getSubject();
        if (subject != null) {
            if (subject.isAuthenticated()) {
                subject.logout();
            }
            ThreadContext.unbindSubject();
        }
        return response;
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
}
