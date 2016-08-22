package com.bazaarvoice.emodb.sor.condition;

public enum Comparison {
    GT("gt"),
    GE("ge"),
    LT("lt"),
    LE("le");

    private final String _deltaFunction;

    private Comparison(String deltaFunction) {
        _deltaFunction = deltaFunction;
    }

    public String getDeltaFunction() {
        return _deltaFunction;
    }
}
