package com.bazaarvoice.emodb.sor.condition;

import com.bazaarvoice.emodb.sor.api.Intrinsic;

/**
 * Equality test on an {@link Intrinsic} data field.
 * <p>
 * Note that {@link Intrinsic#VERSION} is not supported because it's not reliable when combined with weak consistency.
 * Non-quorum reads within a data center or reads in different cross-data centers can compute conflicting version
 * numbers where two reads calculate the same version number but for different content.  Therefore we can't depend on
 * version number when evaluating conditional deltas.
 */
public interface IntrinsicCondition extends Condition {

    /**
     * Returns the name of the intrinsic field to be tested.
     */
    String getName();

    /**
     * Returns the condition to be applied to the value of the intrinsic.
     */
    Condition getCondition();
}
