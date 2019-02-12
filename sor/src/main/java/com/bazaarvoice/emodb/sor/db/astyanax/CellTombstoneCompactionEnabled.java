package com.bazaarvoice.emodb.sor.db.astyanax;

import com.google.inject.BindingAnnotation;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Guice binding annotation for determining whether or not to delete compacted deltas using cell tombstones if the
 * number of blocks in a delta is less than or equal to {@link CellTombstoneBlockLimit}
 */
@BindingAnnotation
@Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
public @interface CellTombstoneCompactionEnabled {
}
