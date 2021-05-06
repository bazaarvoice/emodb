package com.bazaarvoice.emodb.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Optional;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

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
    @JsonProperty("lingerMs")
    private Optional<Long> _lingerMs = Optional.empty();

    private final String producerHealthCheckName = "kafka-producer";

    public Optional<Long> getBufferMemory() {
        return _bufferMemory;
    }

    public Optional<Integer> getBatchsize() {
        return _batchsize;
    }

    public Optional<Long> getLingerMs() {
        return _lingerMs;
    }

    public String getProducerHealthCheckName(){
        return producerHealthCheckName;
    }
}
