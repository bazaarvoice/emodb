package com.bazaarvoice.emodb.sor.condition;

import java.util.Set;

/**
 * Interface for testing list or set containment.
 */
public interface ContainsCondition extends Condition {

    enum Containment {
        ALL("All"),
        ANY("Any"),
        ONLY("Only");

        private final String _suffix;

        private Containment(String suffix) {
            _suffix = suffix;
        }

        public String getSuffix() {
            return _suffix;
        }
    }

    /** The values to test for containment. */
    Set<Object> getValues();

    Containment getContainment();

    /**
     * Returns true if for every value v which matches this condition v also matches the provided condition.
     * For example, containsAll("a","b","c") is a subset of containsAll("a","b"), while containsAll("a","b") is not
     * a subset of containsAll("a","b","c") since v exists, ["a","b"], which matches the former but not the latter.
     */
    boolean isSubsetOf(ContainsCondition condition);

    /**
     * Returns true if there exists a value v which matches both this condition and the provided condition.
     * For example, containsAny("a","b") overlaps containsAny("c","d") since there exists a value, ["a","c"], which
     * matches both, while containsOnly("a","b") does not overlap containsOnly("c","d") since they share no common
     * values.
     */
    boolean overlaps(ContainsCondition condition);
}
