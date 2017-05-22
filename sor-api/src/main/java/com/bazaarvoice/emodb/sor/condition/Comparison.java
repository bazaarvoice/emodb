package com.bazaarvoice.emodb.sor.condition;

import com.bazaarvoice.emodb.sor.delta.Delta;

public enum Comparison {
    GT("gt", false),
    GE("ge", true),
    LT("lt", false),
    LE("le", true);

    private final String _deltaFunction;
    private final boolean _isClosed;

    private Comparison(String deltaFunction, boolean isClosed) {
        _deltaFunction = deltaFunction;
        _isClosed = isClosed;
    }

    /**
     * Returns the name of the this comparison as it appears as a function in the delta syntax.
     * @see Delta#appendTo(Appendable)
     */
    public String getDeltaFunction() {
        return _deltaFunction;
    }

    /**
     * A comparison is closed if the associated value is included in the defined range.  This method returns true for
     * GE and LE, false for GT and LT.
     */
    public boolean isClosed() {
        return _isClosed;
    }
}
