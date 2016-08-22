package com.bazaarvoice.emodb.web.auth.matching;

import com.bazaarvoice.emodb.auth.permissions.matching.ConstantPart;
import com.bazaarvoice.emodb.auth.permissions.matching.MatchingPart;

import java.util.List;

/**
 * Subclass of {@link ConstantPart} with added interface for evaluating Emo-specific permission parts.
 */
public class EmoConstantPart extends ConstantPart implements EmoImplier {

    public EmoConstantPart(String value) {
        super(value);
    }

    @Override
    public boolean impliesCreateTable(CreateTablePart part, List<MatchingPart> leadingParts) {
        // A constant can match a table by name.
        return impliesConstant(new ConstantPart(part.getName()), leadingParts);
    }

    @Override
    public boolean impliesTableCondition(TableConditionPart part, List<MatchingPart> leadingParts) {
        return false;
    }

    @Override
    public boolean impliesCondition(ConditionPart part, List<MatchingPart> leadingParts) {
        return false;
    }
}
