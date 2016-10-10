package com.bazaarvoice.emodb.sor.db.cql;

import com.google.inject.BindingAnnotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Guice annotation for the setting which controls whether to use the CQL driver for multi-get queries, such as:
 * <code>
 *     select * from cat_delta where rowkey in (?,?,?);
 * </code>
 */
@BindingAnnotation
@Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
public @interface CqlForMultiGets {
}
