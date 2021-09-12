package com.bazaarvoice.emodb.sor.compactioncontrol;

import com.google.inject.BindingAnnotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Guice binding annotation for saving the timestamp of the running stash.
 */
@BindingAnnotation
@Target ({FIELD, PARAMETER, METHOD})
@Retention (RUNTIME)
public @interface StashRunTimeMapStore {
}
