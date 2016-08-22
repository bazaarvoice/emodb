package com.bazaarvoice.emodb.auth.proxy;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Optional annotation to clearly mark the credentials parameter in an AuthenticatingProxy.
 */
@Target (PARAMETER) @Retention (RUNTIME)
public @interface Credential {
}
