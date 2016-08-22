package com.bazaarvoice.emodb.auth.legacy;

import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.io.BaseEncoding;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.server.HttpConnection;
import org.eclipse.jetty.server.Request;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.io.IOException;
import java.util.Map;

/**
 * Filter which serves as a bridge between a system that used to use Basic authentication and one that uses API keys.
 * This filter looks for basic credentials and, if they pass, replaces them with the provided API key.
 */
public class BasicToApiKeyAuthenticationFilter implements Filter {

    private final Map<String, String> _basicAuthToApiKeyMap;

    /**
     * Initializes filter with a map of Basic credentials to API keys.  For example:
     *
     * BasicToApiKeyAuthenticationFilter filter = new BasicToApiKeyAuthenticationFilter(
     *     ImmutableMap.of("username:password", "apikey"));
     */
    public BasicToApiKeyAuthenticationFilter(Map<String, String> basicAuthToApiKeyMap) {
        _basicAuthToApiKeyMap = basicAuthToApiKeyMap;
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        // No action required
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        Request httpRequest = (request instanceof Request) ?
                (Request) request :
                HttpConnection.getCurrentConnection().getHttpChannel().getRequest();

        // If there already is an API key present then perform no further action
        String apiKeyHeader = httpRequest.getHeader(ApiKeyRequest.AUTHENTICATION_HEADER);
        String apiKeyParam = httpRequest.getParameter(ApiKeyRequest.AUTHENTICATION_PARAM);
        if (!Strings.isNullOrEmpty(apiKeyHeader) || !Strings.isNullOrEmpty(apiKeyParam)) {
            chain.doFilter(request, response);
            return;
        }

        // If there is no authentication header then perform no further action
        String authenticationHeader = httpRequest.getHeader(HttpHeader.AUTHORIZATION.asString());
        if (Strings.isNullOrEmpty(authenticationHeader)) {
            chain.doFilter(request, response);
            return;
        }

        // Parse the authentication header to determine if it matches the replication user's credentials
        int space = authenticationHeader.indexOf(' ');
        if (space != -1 && "basic".equalsIgnoreCase(authenticationHeader.substring(0, space))) {
            try {
                String credentials = new String(
                        BaseEncoding.base64().decode(authenticationHeader.substring(space+1)), Charsets.UTF_8);

                for (Map.Entry<String, String> entry : _basicAuthToApiKeyMap.entrySet()) {
                    if (entry.getKey().equals(credentials)) {
                        // The user name and password matches the replication credentials.  Insert the header.
                        HttpFields fields = httpRequest.getHttpFields();
                        fields.put(ApiKeyRequest.AUTHENTICATION_HEADER, entry.getValue());
                    }
                }
            } catch (Exception e) {
                // Ok, the header wasn't formatted properly.  Do nothing.
            }
        }

        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
        // No action required
    }
}