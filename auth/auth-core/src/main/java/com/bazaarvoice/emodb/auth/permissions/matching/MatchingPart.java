package com.bazaarvoice.emodb.auth.permissions.matching;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Parent class for all potential parts used by {@link com.bazaarvoice.emodb.auth.permissions.MatchingPermission}.
 */
abstract public class MatchingPart implements Implier {

    // Reserved indexes in each "leadingParts" list
    private final static int CONTEXT = 0;
    private final static int ACTION = 1;
    private final static int RESOURCE = 2;

    abstract protected boolean impliedBy(Implier implier, List<MatchingPart> leadingParts);

    /**
     * Returns true if this part can be included in a permission that is assigned to a user or role.
     * This may return false if support for the part is deprecated or if the part is intended only to test
     * implication by another assignable permission.
     */
    abstract public boolean isAssignable();

    public boolean implies(MatchingPart part, List<MatchingPart> leadingParts) {
        return part.impliedBy(this, leadingParts);
    }

    @Override
    public boolean impliesConstant(ConstantPart part, List<MatchingPart> leadingParts) {
        return false;
    }

    @Override
    public boolean impliesAny() {
        return false;
    }

    public static MatchingPart getContext(List<MatchingPart> leadingParts) {
        return leadingParts.size() > CONTEXT ? leadingParts.get(CONTEXT) : NonePart.instance();
    }

    public static MatchingPart getAction(List<MatchingPart> leadingParts) {
        return leadingParts.size() > ACTION ? leadingParts.get(ACTION) : NonePart.instance();
    }

    public static boolean contextImpliedBy(MatchingPart contextMatcher, List<MatchingPart> leadingParts) {
        return contextMatcher.implies(getContext(leadingParts), ImmutableList.<MatchingPart>of());
    }

    public static boolean actionImpliedBy(MatchingPart actionMatcher, List<MatchingPart> leadingParts) {
        return actionMatcher.implies(getAction(leadingParts), leadingParts.subList(CONTEXT, ACTION));
    }
}