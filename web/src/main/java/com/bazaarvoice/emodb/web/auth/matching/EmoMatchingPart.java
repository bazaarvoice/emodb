package com.bazaarvoice.emodb.web.auth.matching;

import com.bazaarvoice.emodb.auth.permissions.matching.MatchingPart;

import java.util.List;

/**
 * Subclass of {@link MatchingPart} with default implementations for {@link EmoImplier}.
 */
abstract public class EmoMatchingPart extends MatchingPart implements EmoImplier {

    @Override
    public boolean impliesCondition(ConditionPart part, List<MatchingPart> leadingParts) {
        return false;
    }

    @Override
    public boolean impliesTableCondition(TableConditionPart part, List<MatchingPart> leadingParts) {
        return false;
    }

    @Override
    public boolean impliesCreateTable(CreateTablePart part, List<MatchingPart> leadingParts) {
        return false;
    }
}
