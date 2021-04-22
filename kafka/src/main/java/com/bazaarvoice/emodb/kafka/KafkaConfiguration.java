package com.bazaarvoice.emodb.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class KafkaConfiguration {

    @Valid
    @NotNull
    @JsonProperty("bootstrapServers")
    private String _kafkaBootstrapServers;

    @Valid
    @JsonProperty("sasl")
    private SaslConfiguration _saslConfiguration;

    @Valid
    @NotNull
    @JsonProperty("producer")
    private KafkaProducerConfiguration _kafkaProducerConfiguration = new KafkaProducerConfiguration();

    private final String adminHealthCheckName = "kafka-cluster";


    public String getBootstrapServers() {
        return _kafkaBootstrapServers;
    }

    public KafkaProducerConfiguration getKafkaProducerConfiguration() {
        return _kafkaProducerConfiguration;
    }

    public SaslConfiguration getSaslConfiguration() {
        return _saslConfiguration;
    }

    public void setSaslConfiguration(final SaslConfiguration saslConfiguration) {
        _saslConfiguration = saslConfiguration;
    }

    public String getAdminHealthCheckName(){
        return adminHealthCheckName;
    }

}
