package com.bazaarvoice.megabus;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.log.RateLimitedLogFactory;
import com.bazaarvoice.emodb.databus.DatabusOstrichOwnerGroupFactory;
import com.bazaarvoice.emodb.databus.SystemIdentity;
import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.databus.core.DatabusEventStore;
import com.bazaarvoice.emodb.databus.core.DatabusFactory;
import com.bazaarvoice.emodb.event.owner.OstrichOwnerFactory;
import com.bazaarvoice.emodb.event.owner.OstrichOwnerGroupFactory;
import com.bazaarvoice.emodb.event.owner.OwnerGroup;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.ostrich.PartitionContext;
import com.bazaarvoice.ostrich.PartitionContextBuilder;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.Managed;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;

public class MegabusRefProducerManager implements Managed {

    private static final int MAX_PUBLISH_RETRIES = 2;
    private static final String ACKS_CONFIG = "all";

    private static final int NUM_PARTITIONS = 8;

    @Inject
    public MegabusRefProducerManager(final LifeCycleRegistry lifeCycle,
                                  AdminClient adminClient,
                                  @MegabusRefTopic Topic refTopic,
                                  final DatabusFactory databusFactory,
                                  DatabusEventStore databusEventStore,
                                  final @SystemIdentity String systemId,
                                  final RateLimitedLogFactory logFactory,
                                  @DatabusOstrichOwnerGroupFactory OstrichOwnerGroupFactory ownerGroupFactory,
                                  final MetricRegistry metricRegistry,
                                  @BootstrapServers String bootstrapServers,
                                  ObjectMapper objectMapper) {

        createMegabusTopic(adminClient, refTopic);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, ACKS_CONFIG);
        props.put(ProducerConfig.RETRIES_CONFIG, MAX_PUBLISH_RETRIES);

        props.put(ProducerConfig.CLIENT_ID_CONFIG, "megabus-producer");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        Producer<String, JsonNode> producer = new KafkaProducer<String, JsonNode>(props);


        // Since the megabus reads from the databus it must either (a) execute on the same server that owns
        // and manages the databus megabus subscription or (b) go through the Ostrich client that forwards
        // requests to the right server.  This code implements the first option by using the same consistent
        // hash calculation used by Ostrich to determine which server owns the megabus subscription.  As Emo
        // servers join and leave the pool, the OwnerGroup will track which server owns the megabus subscription
        // at a given point in time and start and stop the megabus service appropriately.
        OwnerGroup<Service> ownerGroup = ownerGroupFactory.create("Megabus", new OstrichOwnerFactory<Service>() {
            @Override
            public PartitionContext getContext(String partition) {
                return PartitionContextBuilder.of("Megabus-" + partition);
            }

            @Override
            public Service create(String partition) {
                Databus databus = databusFactory.forOwner(systemId);

                return new MegabusRefProducer(databus, databusEventStore, Conditions.partition(NUM_PARTITIONS, Integer.parseInt(partition)),
                        logFactory, metricRegistry, producer, objectMapper, refTopic, partition);
            }
        }, null);
        lifeCycle.manage(ownerGroup);

        // Start one megabus for each partition
        for (int i = 1; i <= NUM_PARTITIONS; i++) {
            ownerGroup.startIfOwner(Integer.toString(i), Duration.ZERO);
        }
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {
    }

    private void createMegabusTopic(AdminClient adminClient, Topic refTopic) {

        NewTopic newTopic = new NewTopic(refTopic.getName(), refTopic.getPartitions(), refTopic.getReplicationFactor());

        try {
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
        } catch (ExecutionException | InterruptedException e) {
            if (e.getCause() instanceof TopicExistsException) {
                // TODO: check if number of partitions and replication factor match
            } else {
                throw new RuntimeException(e);
            }
        }
    }
}