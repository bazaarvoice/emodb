package com.bazaarvoice.emodb.web.throttling;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Annotation for identifying an API call that needs to be throttled by number of concurrent requests.
 */
@Target({ METHOD }) @Retention(RUNTIME)
public @interface ThrottleConcurrentRequests {
    int maxRequests();
}
