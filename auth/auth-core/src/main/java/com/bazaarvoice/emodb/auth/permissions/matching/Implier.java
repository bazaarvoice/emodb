package com.bazaarvoice.emodb.auth.permissions.matching;

import java.util.List;

/**
 * Visitor interface for determining whether a typed {@link MatchingPart} is implied.
 */
public interface Implier {

    /**
     * Returns true if this instance should permit the provided constant.
     */
    boolean impliesConstant(ConstantPart part, List<MatchingPart> leadingParts);

    /**
     * Returns true if the instance should permit any possible value.  For example a wildcard matcher would return
     * true for the expression "*" since all possible input strings match.
     */
    boolean impliesAny();
}
