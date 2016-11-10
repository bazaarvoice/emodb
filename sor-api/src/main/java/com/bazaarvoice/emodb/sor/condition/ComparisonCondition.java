package com.bazaarvoice.emodb.sor.condition;

public interface ComparisonCondition extends Condition {

    Comparison getComparison();

    Object getValue();

    /**
     * Returns true if there exists a value v which matches both this condition and the provided condition.
     * For example, ge(5) overlaps lt(10) since all values 5 <= v < 10 match both, while ge(5) does not overlap lt(5)
     * since they share no common values.
     */
    boolean overlaps(ComparisonCondition condition);

    /**
     * Returns true if for every value v which matches this condition v also matches the provided condition.
     * For example, gt(10) is a subset of gt(5), while gt(10) is not a subset of lt(20) since all values
     * v >= 20 match the former but not the latter.
     */
    boolean isSubsetOf(ComparisonCondition condition);
}
