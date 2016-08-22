package com.bazaarvoice.emodb.common.dropwizard.metrics;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

/**
 * Variation of {@link com.codahale.metrics.annotation.Timed} where the group name is provided at
 * instantiation time, not hard-coded in the annotation value.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ParameterizedTimed {
    String type() default "";
    String name() default "";
    TimeUnit rateUnit() default TimeUnit.SECONDS;
    TimeUnit durationUnit() default TimeUnit.MILLISECONDS;
}
