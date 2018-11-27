package com.bazaarvoice.emodb.sor.condition.impl;

import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.ConditionVisitor;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.condition.LikeCondition;
import com.bazaarvoice.emodb.sor.delta.deser.DeltaJson;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

abstract public class LikeConditionImpl extends AbstractCondition implements LikeCondition {

    private final String _condition;

    public static LikeConditionImpl create(Object value) {
        if (!(value instanceof String)) {
            throw new IllegalArgumentException("Like expression only supports strings");
        }
        return create(value.toString());
    }

    public static LikeConditionImpl create(final String condition) {
        requireNonNull(condition, "Like expression cannot be null");

        // Optimize for the most common case where an expression contains a single wildcard.
        int firstWildcard = -1;
        List<Integer> remainingWildcards = null;
        String unescaped = condition;

        int length = unescaped.length();
        int i = 0;

        while (i < length) {
            switch (unescaped.charAt(i)) {
                case '\\':
                    if (i == length-1) {
                        throw new IllegalArgumentException("Invalid terminal escape character at position " + i);
                    }
                    // Remove the escape character and preserve the following character.
                    // For example, "abc\\*def" becomes "abc*def" and evaluation of the string
                    // continues at the first character after the '*' ('d').
                    unescaped = unescaped.substring(0, i) + unescaped.substring(i+1);
                    length -= 1;
                    break;

                case '*':
                    // Record the index of the wildcard
                    if (firstWildcard == -1) {
                        firstWildcard = i;
                    } else {
                        if (remainingWildcards == null) {
                            remainingWildcards = new ArrayList<>(3);
                        }
                        remainingWildcards.add(i);
                    }

                    // Consecutive wildcards are redundant.  If there are any remove them now.
                    int endConsecWilds = i+1;
                    while (endConsecWilds != length && unescaped.charAt(endConsecWilds) == '*') {
                        endConsecWilds += 1;
                    }
                    if (endConsecWilds != i+1) {
                        unescaped = unescaped.substring(0, i+1) + unescaped.substring(endConsecWilds);
                        length -= endConsecWilds - i - 1;
                    }

                    break;
            }

            i += 1;
        }

        if (firstWildcard == -1) {
            // There were no wildcards.  Ideally the caller should use a simple equality condition.  We'll
            // optimize by returning a predicate which performs a simple equality check.
            return new ExactMatch(condition, unescaped);
        }

        if (length == 1) {
            // The entire string was nothing but wildcards.  Ideally the caller should use "is(string)" instead.
            return AnyString.getInstance(condition);
        }

        if (remainingWildcards == null) {
            // Simple case where there is exactly one wildcard in the expression
            if (firstWildcard == 0) {
                // Suffix case, such as "*:testcustomer"
                return new EndsWith(condition, unescaped.substring(1));
            } else if (firstWildcard == length-1) {
                // Prefix case, such as "review:*"
                return new StartsWith(condition, unescaped.substring(0, firstWildcard));
            } else {
                // Surrounds case, such as "source:*:testcustomer"
                return new Surrounds(condition, unescaped.substring(0, firstWildcard), unescaped.substring(firstWildcard+1));
            }
        }

        // Multiple wildcards.  The final optimization is the contains case, such as "*review*"
        if (firstWildcard == 0 && remainingWildcards.size() == 1 && remainingWildcards.get(0) == length-1) {
            return new Contains(condition, unescaped.substring(1, length-1));
        }

        // Break the string up into constant substrings separated by wildcards.  Notice that if an expressions
        // starts with a wildcard then the first substring will be the empty string, "".  This is intentional since
        // the empty string will match the beginning of all input strings.  The same logic applies if the
        // expression ends with a wildcard.

        List<String> substrings = new ArrayList<>(remainingWildcards.size() + 2);
        substrings.add(unescaped.substring(0, firstWildcard));
        for (int nextWildcard : remainingWildcards) {
            substrings.add(unescaped.substring(firstWildcard+1, nextWildcard));
            firstWildcard = nextWildcard;
        }
        substrings.add(unescaped.substring(firstWildcard+1));

        return new Complex(condition, substrings);
    }

    protected LikeConditionImpl(String condition) {
        _condition = condition;
    }

    @Override
    public <T, V> V visit(ConditionVisitor<T, V> visitor, @Nullable T context) {
        return visitor.visit(this, context);
    }

    @Override
    public void appendTo(Appendable buf) throws IOException {
        buf.append("like(");
        DeltaJson.append(buf, _condition);
        buf.append(")");
    }

    @Override
    public String getCondition() {
        return _condition;
    }

    @Override
    public boolean overlaps(LikeCondition condition) {
        // If either condition is a constant then the other condition must contain the the condition's string to overlap.
        // For example, "door" overlaps "d*r"
        if (!hasWildcards()) {
            return condition.matches(getCondition());
        } else if (!condition.hasWildcards()) {
            return matches(condition.getCondition());
        }

        // Any internal wildcards surrounded by constants can match any other internal values, so determining overlap
        // only depends on the prefixes and suffixes.

        String prefix = getPrefix();
        String otherPrefix = condition.getPrefix();
        String suffix = getSuffix();
        String otherSuffix = condition.getSuffix();

        return (prefix == null || otherPrefix == null || prefix.startsWith(otherPrefix) || otherPrefix.startsWith(prefix)) &&
                (suffix == null || otherSuffix == null || suffix.endsWith(otherSuffix) || otherSuffix.endsWith(suffix));
    }

    @Override
    public boolean isSubsetOf(LikeCondition condition) {
        // This condition is a subset of the other condition if this condition, with all wildcards replaced with
        // unique characters, matches the other condition.
        String testString = substituteWildcardsWith("\u0000");
        return condition.matches(testString);
    }

    /**
     * Default implementation returns null, subclasses with a prefix must override.
     */
    @Override
    public String getPrefix() {
        return null;
    }

    /**
     * Default implementation returns null, subclasses with a suffix must override.
     */
    @Override
    public String getSuffix() {
        return null;
    }

    /**
     * Default implementation returns true, the one subclass where this is false, {@link ExactMatch}, overrides.
     */
    @Override
    public boolean hasWildcards() {
        return true;
    }
    /**
     * Returns this condition with all wildcards substituted with the provided string.
     */
    abstract protected String substituteWildcardsWith(String substitute);

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LikeCondition)) {
            return false;
        }

        LikeConditionImpl that = (LikeConditionImpl) o;
        return _condition.equals(that._condition);
    }

    @Override
    public int hashCode() {
        return _condition.hashCode();
    }

    /**
     * Returns a simpler equivalent representation of this same condition if one exists.  For example,
     * <code>like("constant_string")</code> can be reduced to the equality condition "constant_string".
     * By default the base class returns itself; subclasses can override as appropriate.
     */
    public Condition simplify() {
        return this;
    }

    /** Implementation for exactly matching a string, such as "review:client" */
    public static class ExactMatch extends LikeConditionImpl {
        private final String _expression;

        private ExactMatch(String condition, String expression) {
            super(condition);
            _expression = expression;
        }

        @Override
        public boolean matches(String input) {
            return _expression.equals(input);
        }

        @Override
        public Condition simplify() {
            return Conditions.equal(_expression);
        }

        @Override
        public boolean hasWildcards() {
            return false;
        }

        @Override
        protected String substituteWildcardsWith(String substitute) {
            return _expression;
        }
    }

    /** Implementation for matching all strings, such as "*" */
    public static class AnyString extends LikeConditionImpl {
        public static AnyString _defaultInstance = new AnyString("*");

        private static AnyString getInstance(String condition) {
            // Most frequently the condition that spawned this instance is a simple single wildcard character,
            // "*".  If this is the case then reuse the default singleton.  Otherwise create a new instance
            // to preserve the original condition.
            if ("*".equals(condition)) {
                return _defaultInstance;
            }
            return new AnyString(condition);
        }

        private AnyString(String condition) {
            super(condition);
        }

        @Override
        public boolean matches(String input) {
            return true;
        }

        @Override
        public Condition simplify() {
            return Conditions.isString();
        }

        @Override
        protected String substituteWildcardsWith(String substitute) {
            return substitute;
        }
    }

    /** Implementation for matching a prefix, such as "review:*" */
    public static class StartsWith extends LikeConditionImpl {
        private final String _prefix;

        private StartsWith(String condition, String prefix) {
            super(condition);
            _prefix = prefix;
        }

        @Override
        public boolean matches(String input) {
            return input.startsWith(_prefix);
        }

        @Override
        public String getPrefix() {
            return _prefix;
        }

        @Override
        protected String substituteWildcardsWith(String substitute) {
            return _prefix + substitute;
        }
    }

    /** Implementation for matching a suffix, such as "*:client" */
    public static class EndsWith extends LikeConditionImpl {
        private final String _suffix;

        private EndsWith(String condition, String suffix) {
            super(condition);
            _suffix = suffix;
        }

        @Override
        public boolean matches(String input) {
            return input.endsWith(_suffix);
        }

        @Override
        public String getSuffix() {
            return _suffix;
        }

        @Override
        protected String substituteWildcardsWith(String substitute) {
            return substitute + _suffix;
        }
    }

    /** Implementation for matching surrounded wildcard, such as "group:*:client" */
    public static class Surrounds extends LikeConditionImpl {
        private final String _prefix;
        private final String _suffix;
        private final int _minLength;

        private Surrounds(String condition, String prefix, String suffix) {
            super(condition);
            _prefix = prefix;
            _suffix = suffix;
            _minLength = _prefix.length() + _suffix.length();
        }

        @Override
        public boolean matches(String input) {
            return input.length() >= _minLength &&
                    input.startsWith(_prefix) &&
                    input.endsWith(_suffix);
        }

        @Override
        public String getPrefix() {
            return _prefix;
        }

        @Override
        public String getSuffix() {
            return _suffix;
        }

        @Override
        protected String substituteWildcardsWith(String substitute) {
            return _prefix + substitute + _suffix;
        }
    }

    /** Implementation for matching a contained expression, such as "*client*" */
    public static class Contains extends LikeConditionImpl {
        private final String _expression;

        private Contains(String condition, String expression) {
            super(condition);
            _expression = expression;
        }

        @Override
        public boolean matches(String input) {
            return input.contains(_expression);
        }

        @Override
        protected String substituteWildcardsWith(String substitute) {
            return substitute + _expression + substitute;
        }
    }

    /**
     * Implementation for matching complex expressions with multiple wildcards that doesn't match
     * any of the previous more efficient computations.
     */
    public static class Complex extends LikeConditionImpl {
        private final String _prefix;
        private final String _suffix;
        private final List<String> _innerSubstrings;
        private final int _minLength;

        private Complex(String condition, List<String> substrings) {
            super(condition);
            int length = substrings.size();
            _prefix = substrings.get(0);
            _suffix = substrings.get(length-1);
            _innerSubstrings = Collections.unmodifiableList(new ArrayList<>(substrings.subList(1, length-1)));

            int minLength = 0;
            for (String substring : substrings) {
                minLength += substring.length();
            }
            _minLength = minLength;
        }

        @Override
        public boolean matches(String input) {
            // Fastest initial checks are whether the total string is at least as long as all substrings
            // followed by a prefix and suffix check
            if (input.length() < _minLength || !input.startsWith(_prefix) || !input.endsWith(_suffix)) {
                return false;
            }

            // Ensure each inner string appears in-order non-overlapping within the input string starting
            // after the prefix.
            int idx = _prefix.length();
            for (String substring : _innerSubstrings) {
                if ((idx = input.indexOf(substring, idx)) == -1) {
                    return false;
                }
                idx += substring.length();
            }

            // Ensure the final inner string terminated before the suffix
            return idx <= input.length() - _suffix.length();
        }

        @Override
        public String getPrefix() {
            return _prefix.length() != 0 ? _prefix : null;
        }

        @Override
        public String getSuffix() {
            return _suffix.length() != 0 ? _suffix : null;
        }

        @Override
        protected String substituteWildcardsWith(String substitute) {
            return _prefix + substitute +
                    _innerSubstrings.stream().collect(Collectors.joining(substitute)) +
                    substitute + _suffix;
        }
    }
}
