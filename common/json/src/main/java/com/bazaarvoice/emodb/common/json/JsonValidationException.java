package com.bazaarvoice.emodb.common.json;

public class JsonValidationException extends IllegalArgumentException {

    public JsonValidationException(String message) {
        super(message);
    }
}
