package com.bazaarvoice.emodb.kafka.health;

import com.bazaarvoice.emodb.common.dropwizard.healthcheck.HealthCheckRegistry;
import com.bazaarvoice.emodb.kafka.BootstrapServers;
import com.bazaarvoice.emodb.kafka.KafkaConfiguration;
import com.codahale.metrics.health.HealthCheck;
import com.google.inject.Inject;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.utils.AppInfoParser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class KafkaAdminHealthCheck extends HealthCheck {
    private final AdminClient adminClient;
    private final String bootstrapServers;
    private static final String KAFKA_CLIENT_VERSION = AppInfoParser.getVersion();

    @Inject
    public KafkaAdminHealthCheck(final AdminClient adminClient,
                                 @BootstrapServers String bootstrapServers,
                                 final HealthCheckRegistry healthCheckRegistry,
                                 final KafkaConfiguration healthCheckConfiguration) {
        this.adminClient = requireNonNull(adminClient);
        this.bootstrapServers = requireNonNull(bootstrapServers);
        healthCheckRegistry.addHealthCheck(healthCheckConfiguration.getAdminHealthCheckName(), this);
    }

    @Override
    protected Result check() throws Exception {
        try {

            final DescribeClusterResult response = adminClient.describeCluster();

            final boolean nodesNotEmpty = !response.nodes().get().isEmpty();
            final boolean clusterIdAvailable = response.clusterId() != null;
            final boolean aControllerExists = response.controller().get() != null;

            final List<String> errors = new ArrayList<>();

            if (!nodesNotEmpty) {
                errors.add("no nodes found for " + bootstrapServers);
            }

            if (!clusterIdAvailable) {
                errors.add("no cluster id available for " + bootstrapServers);
            }

            if (!aControllerExists) {
                errors.add("no active controller exists for " + bootstrapServers);
            }

            if (!errors.isEmpty()) {
                final String errorMessage = String.join(",", errors);
                return Result.unhealthy(errorMessage);
            }

            Collection<Node> nodeCount = response.nodes().get();
            Optional<Node> nodeDetails = nodeCount.stream().findFirst();

            return Result.healthy("Kafka client version: " + KAFKA_CLIENT_VERSION + " | Node count: " + nodeCount.size() +
                    " | Node(1) info: Host - " + nodeDetails.map(Node::host).orElse(null) +
                    ". Port - " + nodeDetails.map(Node::port).orElse(null));
        } catch (final Exception e) {
            return Result.unhealthy("Error describing Kafka Cluster, servers=" + bootstrapServers + " error: " + e);
        }
    }
}
