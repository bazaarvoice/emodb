package com.bazaarvoice.emodb.databus;

import com.google.inject.BindingAnnotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Guice binding annotation for identifying the condition for suppressing events on the databus when
 * <code>ignoreSuppressedEvents</code> is enabled for a subscription.
 *
 * @see com.bazaarvoice.emodb.databus.api.Databus#subscribe(String, com.bazaarvoice.emodb.sor.condition.Condition, org.joda.time.Duration, org.joda.time.Duration, boolean)
 */
@BindingAnnotation
@Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
public @interface SuppressedEventCondition {
}
