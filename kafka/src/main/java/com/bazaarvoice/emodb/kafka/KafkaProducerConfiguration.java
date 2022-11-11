package com.bazaarvoice.emodb.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Optional;

public class KafkaProducerConfiguration {
    @Valid
    @NotNull
    @JsonProperty("bufferMemory")
    private Optional<Long> _bufferMemory = Optional.empty();

    @Valid
    @NotNull
    @JsonProperty("batchSize")
    private Optional<Integer> _batchsize = Optional.empty();

    @Valid
    @NotNull
    @JsonProperty("maxRequestSize")
    private Integer _maxRequestSize = 15 * 1024 * 1024; // 15MB

    @Valid
    @NotNull
    @JsonProperty("lingerMs")
    private Optional<Long> _lingerMs = Optional.empty();

    private final String producerHealthCheckName = "kafka-producer";

    public Optional<Long> getBufferMemory() {
        return _bufferMemory;
    }

    public Optional<Integer> getBatchsize() {
        return _batchsize;
    }


    public Integer getMaxRequestSize() {
        return _maxRequestSize;
    }

    public Optional<Long> getLingerMs() {
        return _lingerMs;
    }

    public String getProducerHealthCheckName() {
        return producerHealthCheckName;
    }
}
