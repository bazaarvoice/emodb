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
    @JsonProperty("ssl")
    private SslConfiguration _sslConfiguration;

    @Valid
    @NotNull
    @JsonProperty("producer")
    private KafkaProducerConfiguration _kafkaProducerConfiguration = new KafkaProducerConfiguration();

    public String getBootstrapServers() {
        return _kafkaBootstrapServers;
    }

    public KafkaProducerConfiguration getKafkaProducerConfiguration() {
        return _kafkaProducerConfiguration;
    }

    public SslConfiguration getSslConfiguration() {
        return _sslConfiguration;
    }

    public void setSslConfiguration(final SslConfiguration sslConfiguration) {
        _sslConfiguration = sslConfiguration;
    }
}
