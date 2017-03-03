package com.bazaarvoice.emodb.web.compactioncontrol;


import com.google.inject.BindingAnnotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Guice binding annotation indicating that the annotated object is the list of all Compaction control sources, local as well as remotes.
 */
@BindingAnnotation
@Target ({FIELD, PARAMETER, METHOD})
@Retention (RUNTIME)
public @interface AllCompactionControlSources {
}
