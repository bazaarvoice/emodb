package com.bazaarvoice.emodb.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class KafkaConfiguration {

    @Valid
    @NotNull
    @JsonProperty("bootstrapServers")
    private String _kafkaBootstrapServers;

    public String getBootstrapServers() {
        return _kafkaBootstrapServers;
    }
}
