package com.bazaarvoice.emodb.web.auth.matching;

import com.bazaarvoice.emodb.auth.permissions.matching.AnyPart;
import com.bazaarvoice.emodb.auth.permissions.matching.MatchingPart;

import java.util.List;

/**
 * Subclass of {@link AnyPart} with added interface for implying all Emo-specific permission parts.
 */
public class EmoAnyPart extends AnyPart implements EmoImplier {

    private final static EmoAnyPart INSTANCE = new EmoAnyPart();

    public static EmoAnyPart instance() {
        return INSTANCE;
    }

    @Override
    public boolean impliesCondition(ConditionPart part, List<MatchingPart> leadingParts) {
        return true;
    }

    @Override
    public boolean impliesTableCondition(TableConditionPart part, List<MatchingPart> leadingParts) {
        return true;
    }

    @Override
    public boolean impliesCreateTable(CreateTablePart part, List<MatchingPart> leadingParts) {
        return true;
    }
}
