package com.bazaarvoice.emodb.sor.condition;

import javax.annotation.Nullable;

public interface LikeCondition extends Condition {

    /**
     * Returns the matching string used by this condition.
     */
    String getCondition();

    /**
     * Returns true if the provided String is a match for this condition.
     */
    boolean matches(String input);

    /**
     * Returns true if there exists a value v which matches both this condition and the provided condition.
     * For example, like("a*") overlaps like("*c") since there exists a value, "abc", which matches both,
     * while like("a*") does not overlap like("b*") since they share no common values.
     */
    boolean overlaps(LikeCondition condition);

    /**
     * Returns true if for every value v which matches this condition v also matches the provided condition.
     * For example, like("ab*") is a subset of like("a*"), while like("a*") is not a subset of like("*c") since
     * there exists a value, "ab", which matches the former but not the latter.
     */
    boolean isSubsetOf(LikeCondition condition);

    /**
     * Returns the constant prefix shared by all results matching this condition, or null if no such prefix exists.
     * For example:  "ab*cd" has prefix "ab" and "*cd" has prefix null.
     */
    @Nullable
    String getPrefix();

    /**
     * Returns the constant suffix shared by all results matching this condition, or null if no such suffix exists.
     * For example:  "ab*cd" has suffix "cd" and "ab*" has suffix null.
     */
    @Nullable
    String getSuffix();

    /**
     * Returns true if the condition contains any wildcards, false if it is a constant.
     */
    boolean hasWildcards();
}
