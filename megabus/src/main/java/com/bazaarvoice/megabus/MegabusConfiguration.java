package com.bazaarvoice.megabus;

import com.bazaarvoice.emodb.kafka.Topic;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class MegabusConfiguration {

    @Valid
    @NotNull
    @JsonProperty("kafkaBootstrapServers")
    private String _kafkaBootstrapServers;

    @Valid
    @NotNull
    @JsonProperty("megabusRefTopic")
    private Topic _megabusRefTopic;

    @Valid
    @NotNull
    @JsonProperty("megabusTopic")
    private Topic _megabusTopic;

    public String getKafkaBootstrapServers() {
        return _kafkaBootstrapServers;
    }

    public Topic getMegabusRefTopic() {
        return _megabusRefTopic;
    }

    public Topic getMegabusTopic() {
        return _megabusTopic;
    }
}
