package com.bazaarvoice.emodb.auth.permissions.matching;

import java.util.List;

/**
 * MatchingPart that never matches any other values.  This part is only used internally to return a part that
 * matches no results instead of returning null when no part exists.
 */
public class NonePart extends MatchingPart {

    private static final NonePart INSTANCE = new NonePart();

    public static NonePart instance() {
        return INSTANCE;
    }

    protected NonePart() {
        // empty
    }

    @Override
    protected boolean impliedBy(Implier implier, List<MatchingPart> leadingParts) {
        // Nothing implies this part; always return false
        return false;
    }

    @Override
    public boolean isAssignable() {
        return false;
    }

    // None implies no values, so the Implier interface returns false for all values.

    @Override
    public boolean impliesConstant(ConstantPart part, List<MatchingPart> leadingParts) {
        return false;
    }

    @Override
    public boolean impliesAny() {
        return false;
    }
}
