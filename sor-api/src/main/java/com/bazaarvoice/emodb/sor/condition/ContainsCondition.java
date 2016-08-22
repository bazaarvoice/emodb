package com.bazaarvoice.emodb.sor.condition;

import java.util.Set;

/**
 * Interface for testing list or set containment.
 */
public interface ContainsCondition extends Condition {

    public enum Containment {
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
}
