package com.bazaarvoice.megabus.refproducer;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class MegabusRefProducerConfiguration {

    @Valid
    @NotNull
    @JsonProperty("batchSize")
    private int _batchSize = 500;

    @Valid
    @NotNull
    @JsonProperty("skipWaitThreshold")
    private int _skipWaitThreshold = 500;

    @Valid
    @NotNull
    @JsonProperty("pollIntervalMs")
    private int _pollIntervalMs = 100;

    public int getBatchSize() {
        return _batchSize;
    }

    public int getSkipWaitThreshold() {
        return _skipWaitThreshold;
    }

    public int getPollIntervalMs() {
        return _pollIntervalMs;
    }
}
