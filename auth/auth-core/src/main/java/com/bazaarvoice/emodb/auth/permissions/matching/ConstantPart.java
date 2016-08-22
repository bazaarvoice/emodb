package com.bazaarvoice.emodb.auth.permissions.matching;

import java.util.List;
import java.util.Objects;

/**
 * MatchingPart implementation for matching a single constant value.  The default implementation only implies
 * other ConstantParts with the exact same value.
 */
public class ConstantPart extends MatchingPart {
    private final String _value;

    public ConstantPart(String value) {
        _value = value;
    }

    public String getValue() {
        return _value;
    }

    @Override
    protected boolean impliedBy(Implier implier, List<MatchingPart> leadingParts) {
        return implier.impliesConstant(this, leadingParts);
    }

    @Override
    public boolean impliesConstant(ConstantPart part, List<MatchingPart> leadingParts) {
        return Objects.equals(getValue(), part.getValue());
    }

    @Override
    public boolean isAssignable() {
        return true;
    }
}
