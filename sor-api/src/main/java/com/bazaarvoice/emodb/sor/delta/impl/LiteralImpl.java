package com.bazaarvoice.emodb.sor.delta.impl;

import com.bazaarvoice.emodb.common.json.JsonValidator;
import com.bazaarvoice.emodb.common.json.OrderedJson;
import com.bazaarvoice.emodb.sor.delta.DeltaVisitor;
import com.bazaarvoice.emodb.sor.delta.Literal;
import com.bazaarvoice.emodb.sor.delta.deser.DeltaJson;
import com.google.common.base.Throwables;
import com.google.common.io.CharStreams;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class LiteralImpl extends AbstractDelta implements Literal {

    @Nullable
    private final Object _value;

    public LiteralImpl(@Nullable Object value) {
        _value = JsonValidator.checkValid(value);
    }

    @Override
    @Nullable
    public Object getValue() {
        return _value;
    }

    @Override
    @Nullable
    public <T, V> V visit(DeltaVisitor<T, V> visitor, @Nullable T context) {
        return visitor.visit(this, context);
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    @Override
    public void appendTo(Appendable buf) throws IOException {
        // nested content is sorted so toString() is deterministic.  this is valuable in tests and for
        // comparing hashes of deltas for equality.
        DeltaJson.write(CharStreams.asWriter(buf), OrderedJson.ordered(_value));
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(@Nullable Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof Literal)) {
            return false;
        }

        Object that = ((Literal) obj).getValue();
        if (_value == null) {
            return that == null;
        } else if (that == null) {
            return false;
        }

        // Optimize for the common case where values being equated are the same type
        Class<?> thisClass = _value.getClass();
        Class<?> thatClass = that.getClass();

        if (!areClassesEqual(thisClass, thatClass)) {
            // If both values are numeric then use numeric equality regardless of the underlying type (int, double, and so on)
            if (isNumber(thisClass) && isNumber(thatClass)) {
                if (isDouble((Class<? extends Number>) thisClass) || isDouble((Class<? extends Number>) thatClass)) {
                    return ((Number) _value).doubleValue() == ((Number) that).doubleValue();
                } else {
                    return ((Number) _value).longValue() == ((Number) that).longValue();
                }
            }
        }

        return _value.equals(that);
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compareTo(Literal o) {
        Object that = o.getValue();

        // Sort nulls first
        if (_value == null) {
            return that == null ? 0 : -1;
        } else if (that == null) {
            return 1;
        }

        // Optimize for common case where values being compared are of the same type
        Class<?> thisClass = _value.getClass();
        Class<?> thatClass = that.getClass();

        if (!areClassesEqual(thisClass, thatClass)) {
            // Values are of different types
            // If both values are numeric then use numeric comparison regardless of the underlying type (int, double, and so on)
            if (isNumber(thisClass) && isNumber(thatClass)) {
                if (isDouble((Class<? extends Number>) thisClass) || isDouble((Class<? extends Number>) thatClass)) {
                    return Doubles.compare(((Number) _value).doubleValue(), ((Number) that).doubleValue());
                } else {
                    return Longs.compare(((Number) _value).longValue(), ((Number) that).longValue());
                }
            }

            // Sort by simple class name
            return getComparisonClassName(thisClass).compareTo(getComparisonClassName(thatClass));
        }

        if (Comparable.class.isAssignableFrom(thisClass)) {
            return ((Comparable) _value).compareTo(that);
        }

        // When not comparable, compare the string representations
        try {
            StringBuilder thisStr = new StringBuilder();
            StringBuilder thatStr = new StringBuilder();
            this.appendTo(thisStr);
            o.appendTo(thatStr);
            return thisStr.toString().compareTo(thatStr.toString());
        } catch (IOException e) {
            // Should never happen
            Throwables.propagateIfPossible(e);
            throw new RuntimeException(e);
        }

    }

    /**
     * When comparing literals allow subclasses of Maps and Lists to be directly compared even if they have
     * different implementations.
     */
    private boolean areClassesEqual(Class<?> c1, Class<?> c2) {
        if (isMap(c1)) {
            return isMap(c2);
        } else if (isList(c1)) {
            return isList(c2);
        } else {
            return c1.equals(c2);
        }
    }

    /**
     * When comparing literals with different types sort them by their simple class names.  Normalize numbers,
     * maps, and lists so that all values of each type are sorted together regardless of actual implementation
     * (Integer vs Float, HashMap vs TreeMap, and so on).
     */
    private String getComparisonClassName(Class<?> clazz) {
        if (isNumber(clazz)) {
            return Number.class.getSimpleName();
        } else if (isMap(clazz)) {
            return Map.class.getSimpleName();
        } else if (isList(clazz)) {
            return List.class.getSimpleName();
        }
        return clazz.getSimpleName();
    }

    private boolean isNumber(Class<?> clazz) {
        return Number.class.isAssignableFrom(clazz);
    }

    private boolean isDouble(Class<? extends Number> clazz) {
        return Double.class.equals(clazz) || Float.class.equals(clazz);
    }

    private boolean isMap(Class<?> clazz) {
        return Map.class.isAssignableFrom(clazz);
    }

    private boolean isList(Class<?> clazz) {
        return List.class.isAssignableFrom(clazz);
    }

    @Override
    public int hashCode() {
        return 8971 ^ (_value != null ? _value.hashCode() : 0);
    }
}
