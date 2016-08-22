package com.bazaarvoice.emodb.sor.condition;

/**
 * Tests that {@link IsCondition} can perform.
 */
public enum State {
    UNDEFINED,
    DEFINED,
    NULL,
    BOOL,
    NUM,
    STRING,
    ARRAY,
    OBJECT,
}
