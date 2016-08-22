package com.bazaarvoice.emodb.web.throttling;

import com.bazaarvoice.emodb.common.zookeeper.store.MapStore;
import com.google.common.base.CharMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/** Blacklists clients based on a list of IP addresses stored in ZooKeeper. */
public class BlackListedIpFilter implements Filter {
    private static final Logger _log = LoggerFactory.getLogger(BlackListedIpFilter.class);

    private static final CharMatcher IP_CHARS =
            CharMatcher.DIGIT.or(CharMatcher.anyOf(".:%")).precomputed();  // allow ipv4 and ipv6

    private final MapStore<Long> _blackListIpStore;

    public BlackListedIpFilter(MapStore<Long> blackListIpStore) {
        _blackListIpStore = blackListIpStore;
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        // Do nothing
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        // If Dropwizard HTTPConfiguration.useForwardedHeaders is set (as it is by default) then
        // request.getRemoteAddr() will be the X-Forwarded-For header set by Amazon ELB, if present.
        String userIpAddress = request.getRemoteAddr();

        // Skip the blacklist check if the IP addresses might be an illegal key for the ZK blacklist.
        if (userIpAddress != null && isBlackListed(userIpAddress))  {
            ((HttpServletResponse) response).sendError(HttpServletResponse.SC_FORBIDDEN,
                    String.format("Requesting IP %s is blacklisted. Please try again later.", userIpAddress));
            return;
        }

        chain.doFilter(request, response);
    }

    private boolean isBlackListed(String userIpAddress) {
        // Skip the blacklist check if the IP addresses might be an illegal key for the ZK blacklist.
        if (!IP_CHARS.matchesAllOf(userIpAddress)) {
            return false;
        }
        Long expireAt = _blackListIpStore.get(userIpAddress);
        if (expireAt == null) {
            return false;
        }
        if (expireAt < System.currentTimeMillis()) {
            // Remove from black list if the item is expired
            try {
                _blackListIpStore.remove(userIpAddress);
                _log.info("ip black list entry has expired: {}", userIpAddress);
            } catch (Exception e) {
                _log.error(e.toString());
            }
            return false;
        }
        return true;
    }

    @Override
    public void destroy() {
        // Do nothing
    }
}
