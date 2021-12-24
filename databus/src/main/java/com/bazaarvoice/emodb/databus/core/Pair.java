package com.bazaarvoice.emodb.databus.core;

import java.util.Objects;

import static java.util.Objects.hash;

/**
 * Package private to make it less likely that Pair will proliferate.  For discussion about why Pairs should be used
 * sparingly, see http://code.google.com/p/guava-libraries/wiki/IdeaGraveyard.
 */
class Pair<S, T> {
    private final S _first;
    private final T _second;

    static <S, T> Pair<S, T> of(S first, T second) {
        return new Pair<>(first, second);
    }

    private Pair(S first, T second) {
        _first = first;
        _second = second;
    }

    S first() {
        return _first;
    }

    T second() {
        return _second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Pair)) {
            return false;
        }
        Pair pair = (Pair) o;
        return Objects.equals(_first, pair._first) && Objects.equals(_second, pair._second);
    }

    @Override
    public int hashCode() {
        return hash(_first, _second);
    }

    @Override
    public String toString() {
        return "[" + _first + "," + _second + "]";  // for debugging
    }
}
