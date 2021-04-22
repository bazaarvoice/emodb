package com.bazaarvoice.emodb.kafka.health;

import com.bazaarvoice.emodb.common.dropwizard.healthcheck.HealthCheckRegistry;
import com.bazaarvoice.emodb.kafka.KafkaCluster;
import com.bazaarvoice.emodb.kafka.KafkaConfiguration;
import com.codahale.metrics.health.HealthCheck;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Collection;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class KafkaProducerHealthCheck extends HealthCheck {

    private final Producer producer;
    private final Collection<String> topics;
    private final String KAFKA_VERSION = "2.3.0";


    @Inject
    public KafkaProducerHealthCheck(final KafkaCluster kafkaCluster,
                                    final HealthCheckRegistry healthCheckRegistry,
                                    final KafkaConfiguration healthCheckConfiguration) {
        this.producer = requireNonNull(kafkaCluster.producer());
        this.topics = requireNonNull(kafkaCluster.getAllTopics());
        healthCheckRegistry.addHealthCheck(healthCheckConfiguration.getProducerName(), this);
    }

    @Override
    protected Result check() {
        try {
            topics.forEach(producer::partitionsFor);
            return Result.healthy(" Kafka version: "+KAFKA_VERSION);
        } catch (Exception e) {
            return Result.unhealthy(e);
        }
    }
}
