package com.bazaarvoice.emodb.web.jersey;

import com.google.common.base.Stopwatch;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * This filter is intended for responses which return a continuous stream of data but at a potentially slow rate.
 * The filtered output stream ensures the response is flushed every 100ms.  This is not done asynchronously but is
 * analyzed as the response is written.  Therefore to achieve the full benefit of this filter the resource should
 * continuously write to the stream, such as with non-entity breaking whitespace, so there will always be data in the
 * buffer to flush.
 */
public class UnbufferedStreamFilter implements Filter {

    public final static String UNBUFFERED_HEADER = "X-BV-Unbuffered";

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        // Do nothing
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        HttpServletResponse wrappedResponse = new HttpServletResponseWrapper((HttpServletResponse) response) {
            @Override
            public ServletOutputStream getOutputStream() throws IOException {
                if (((HttpServletResponse) getResponse()).getHeader(UNBUFFERED_HEADER) == null) {
                    return getResponse().getOutputStream();
                }

                return new ServletOutputStream() {
                    Stopwatch flushStopwatch = Stopwatch.createStarted();

                    @Override
                    public void write(int b) throws IOException {
                        getResponse().getOutputStream().write(b);
                        maybeFlush();
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        getResponse().getOutputStream().write(b, off, len);
                        maybeFlush();
                    }

                    private void maybeFlush() throws IOException {
                        if (flushStopwatch.elapsed(TimeUnit.MILLISECONDS) >= 100) {
                            getResponse().flushBuffer();
                            flushStopwatch.reset().start();
                        }
                    }

                    @Override
                    public boolean isReady() {
                        return true;
                    }

                    @Override
                    public void setWriteListener(WriteListener writeListener) {
                        // No-op because isReady() always returns true
                    }
                };
            }
        };

        chain.doFilter(request, wrappedResponse);
    }

    @Override
    public void destroy() {
        // Do nothing
    }
}
