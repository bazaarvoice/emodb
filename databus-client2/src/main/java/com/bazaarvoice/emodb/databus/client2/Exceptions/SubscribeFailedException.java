package com.bazaarvoice.emodb.databus.client2.Exceptions;

public class SubscribeFailedException extends RuntimeException {

    private final int _responseCode;
    private final String _content;
    private final Exception _cause;

    public SubscribeFailedException(String subscription, int responseCode, String content) {
        super("Subscribe request failed for subscription \"" + subscription + "\"");
        _responseCode = responseCode;
        _content = content;
        _cause = null;
    }

    public SubscribeFailedException(String subscription, Exception cause) {
        super("Subscribe request failed for subscription \"" + subscription + "\"");
        _responseCode = -1;
        _content = cause.getMessage();
        _cause = cause;
    }
    public int getResponseCode() {
        return _responseCode;
    }

    public String getContent() {
        return _content;
    }

    @Override
    public Exception getCause() {
        return _cause;
    }
}
