package com.bazaarvoice.emodb.web.auth.matching;

import com.bazaarvoice.emodb.auth.permissions.matching.Implier;
import com.bazaarvoice.emodb.auth.permissions.matching.MatchingPart;

import java.util.List;

/**
 * Special reserved permission part used to check whether there is <em>some</em> part which would be implied
 * by another part.
 *
 * For example, assume we want to check whether a user has permission to read from at least one table but is not
 * concerned about <em>which</em> table the user can read.  This part can be used to make that check.  For example:
 *
 * "sor|*|this:table:only" implies "sor|read|?" == true
 *
 *     This is true because there is at least one permission,"sor|read|this:table:only", which is implied.  Note that
 *     it returns true regardless of whether the table "this:table:only" actually exists.
 *
 * "sor|update|*" implies "sor|read|?" == false
 *
 *     This is false because there are no read permissions implied by the permission.
 * 
 */
public class EmoImpliedPartExistsPart extends EmoMatchingPart {

    private final static EmoImpliedPartExistsPart INSTANCE = new EmoImpliedPartExistsPart();

    public static EmoImpliedPartExistsPart instance() {
        return INSTANCE;
    }

    @Override
    protected boolean impliedBy(Implier implier, List<MatchingPart> leadingParts) {
        return ((EmoImplier) implier).impliedPartExists();
    }

    @Override
    public boolean impliedPartExists() {
        // This part check cannot imply another
        return false;
    }

    @Override
    public boolean isAssignable() {
        return false;
    }
}
