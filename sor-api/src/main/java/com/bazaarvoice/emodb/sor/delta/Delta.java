package com.bazaarvoice.emodb.sor.delta;

import com.bazaarvoice.emodb.sor.delta.deser.DeltaDeserializer;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * A set of changes that can be applied to update JSON data.
 * <p>
 * Use {@link Deltas} to create {@code Delta} objects.
 */
@JsonDeserialize(using = DeltaDeserializer.class)
public interface Delta {

    <T, V> V visit(DeltaVisitor<T, V> visitor, @Nullable T context);

    /**
     * Returns true if this delta replaces the old value with a new value without regard to
     * what the old value is.  In other words, returns true if the delta evaluator for this
     * delta always returns the same thing no matter what the input is.
     */
    boolean isConstant();

    @JsonValue
    String toString();

    void appendTo(Appendable buf) throws IOException;

    boolean equals(@Nullable Object o);

    int hashCode();

    /**
     * Returns the size of the delta when converted into a string
     */
    int size();
}
