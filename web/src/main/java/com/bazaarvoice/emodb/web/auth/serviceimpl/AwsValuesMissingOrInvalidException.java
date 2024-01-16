package com.bazaarvoice.emodb.web.auth.serviceimpl;

public class AwsValuesMissingOrInvalidException extends RuntimeException{
    public AwsValuesMissingOrInvalidException(String message) {
        super(message);
    }
}
