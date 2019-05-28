package com.bazaarvoice.emodb.kafka;

import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.TopicExistsException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DefaultKafkaCluster implements KafkaCluster {

    private final AdminClient _adminClient;
    private final String _bootstrapServers;

    @Inject
    public DefaultKafkaCluster(AdminClient adminClient, @BootstrapServers String bootstrapServers) {
        _adminClient = checkNotNull(adminClient);
        _bootstrapServers = checkNotNull(bootstrapServers);

        Futures.getUnchecked(_adminClient.describeCluster().nodes()).forEach(System.out::println);
    }

    @Override
    public void createTopicIfNotExists(Topic topic) {
        NewTopic newTopic = new NewTopic(topic.getName(), topic.getPartitions(), topic.getReplicationFactor());

        try {
            _adminClient.createTopics(Collections.singleton(newTopic)).all().get();
        } catch (ExecutionException | InterruptedException e) {
            if (e.getCause() instanceof TopicExistsException) {
                checkTopicPropertiesMatching(topic);
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private void checkTopicPropertiesMatching(Topic topic) {
            TopicDescription topicDescription = Futures.getUnchecked(
                    _adminClient.describeTopics(Collections.singleton(topic.getName())).all()).get(topic.getName());

            checkArgument(topicDescription.partitions().size() == topic.getPartitions());
            topicDescription.partitions().forEach(topicPartitionInfo ->
                    checkArgument(topicPartitionInfo.replicas().size() == topic.getReplicationFactor()));
    }

    @Override
    public String getBootstrapServers() {
        return _bootstrapServers;
    }
}
