package com.bazaarvoice.emodb.sor.db.astyanax;

import com.google.inject.BindingAnnotation;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Guice binding annotation for identifying the minimum number of blocks in a delta for it to be compacted using a
 * range tombstone rather than a cell tombstone.
 */
@BindingAnnotation
@Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
public @interface CellTombstoneBlockLimit {
}