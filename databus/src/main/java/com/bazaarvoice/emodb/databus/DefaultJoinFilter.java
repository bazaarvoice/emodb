package com.bazaarvoice.emodb.databus;

import com.google.inject.BindingAnnotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Guice binding annotation for identifying the configured condition to be joined to all databus subscriptions by
 * default.  A subscriber may choose to decline adding the assisted filter to his subscription by enabling
 * <code>includeDefaultJoinFilter</code> upon subscription.
 *
 * @see com.bazaarvoice.emodb.databus.api.Databus#subscribe(String, com.bazaarvoice.emodb.sor.condition.Condition, java.time.Duration, java.time.Duration, boolean)
 */
@BindingAnnotation
@Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
public @interface DefaultJoinFilter {
}
