package com.bazaarvoice.emodb.kafka.health;

import com.bazaarvoice.emodb.common.dropwizard.healthcheck.HealthCheckRegistry;
import com.bazaarvoice.emodb.kafka.KafkaCluster;
import com.bazaarvoice.emodb.kafka.KafkaConfiguration;
import com.codahale.metrics.health.HealthCheck;
import com.google.inject.Inject;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.utils.AppInfoParser;

import java.util.Collection;

import static java.util.Objects.requireNonNull;

public class KafkaProducerHealthCheck extends HealthCheck {

    private final Producer producer;
    private final Collection<String> topics;
    private static final String KAFKA_CLIENT_VERSION = AppInfoParser.getVersion();


    @Inject
    public KafkaProducerHealthCheck(final KafkaCluster kafkaCluster,
                                    final HealthCheckRegistry healthCheckRegistry,
                                    final KafkaConfiguration healthCheckConfiguration) {
        this.producer = requireNonNull(kafkaCluster.producer());
        this.topics = requireNonNull(kafkaCluster.getAllTopics());

        healthCheckRegistry.addHealthCheck(healthCheckConfiguration.
                getKafkaProducerConfiguration().getProducerHealthCheckName(), this);
    }

    @Override
    protected Result check() {
        try {
            topics.forEach(producer::partitionsFor);
            return Result.healthy("Kafka client version: " + KAFKA_CLIENT_VERSION + " | Topics: " + topics.toString());
        } catch (Exception e) {
            return Result.unhealthy(e);
        }
    }
}
