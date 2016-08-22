package com.bazaarvoice.emodb.web.auth.matching;

import com.bazaarvoice.emodb.auth.permissions.matching.Implier;
import com.bazaarvoice.emodb.auth.permissions.matching.MatchingPart;

import java.util.List;

/**
 * Sub-interface of {@link Implier} with added methods for evaluating implication of Emo-specific permission parts.
 * All MatchingParts returned by EmoPermission must implement this interface.
 */
public interface EmoImplier extends Implier {

    /**
     * Returns true if the instance should permit the provided condition.  This method is here for completeness;
     * conditions are intended to be used to evaluate implication of other other parts.  Most implementations will
     * always return false.
     */
    boolean impliesCondition(ConditionPart part, List<MatchingPart> leadingParts);

    /**
     * Returns true if the instance should permit the provided table condition.  This method is here for completeness;
     * table conditions are intended to be used to evaluate implication of other other parts.  Most implementations will
     * always return false.
     */
    boolean impliesTableCondition(TableConditionPart part, List<MatchingPart> leadingParts);

    /**
     * Returns true if the instance should permit creating the provided table.
     */
    boolean impliesCreateTable(CreateTablePart part, List<MatchingPart> leadingParts);
}
