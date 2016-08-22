package com.bazaarvoice.emodb.web.resources;

import com.fasterxml.jackson.annotation.JsonInclude;

import javax.annotation.Nullable;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class SuccessResponse {

    private static final SuccessResponse INSTANCE = new SuccessResponse(null);

    @Nullable
    private final Map<String, ?> _debug;

    public static SuccessResponse instance() {
        return INSTANCE;
    }

    private SuccessResponse(Map<String, ?> debug) {
        _debug = debug;
    }

    public SuccessResponse with(Map<String, ?> debug) {
        return new SuccessResponse(debug);
    }

    public boolean isSuccess() {
        return true;
    }

    @Nullable
    public Map<String, ?> getDebug() {
        return _debug;
    }
}
