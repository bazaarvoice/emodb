package com.bazaarvoice.emodb.auth.permissions.matching;

import java.util.List;

/**
 * Efficient MatchingPart implementation which matches all possible other parts.
 */
public class AnyPart extends MatchingPart {

    private static final AnyPart INSTANCE = new AnyPart();

    public static AnyPart instance() {
        return INSTANCE;
    }

    protected AnyPart() {
        // empty
    }

    @Override
    protected boolean impliedBy(Implier implier, List<MatchingPart> leadingParts) {
        return implier.impliesAny();
    }

    @Override
    public boolean isAssignable() {
        return true;
    }

    // Any implies all values, so the Implier interface returns true for all values.

    @Override
    public boolean impliesConstant(ConstantPart part, List<MatchingPart> leadingParts) {
        return true;
    }

    @Override
    public boolean impliesAny() {
        return true;
    }
}
