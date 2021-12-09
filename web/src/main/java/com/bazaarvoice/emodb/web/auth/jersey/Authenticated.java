package com.bazaarvoice.emodb.web.auth.jersey;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target ({ElementType.PARAMETER, ElementType.FIELD})
@Retention (RetentionPolicy.RUNTIME)
@Documented
public @interface Authenticated {
}
