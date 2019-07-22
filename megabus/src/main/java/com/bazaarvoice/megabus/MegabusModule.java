package com.bazaarvoice.megabus;

import com.bazaarvoice.emodb.common.dropwizard.log.DefaultRateLimitedLogFactory;
import com.bazaarvoice.emodb.common.dropwizard.log.RateLimitedLogFactory;
import com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode;
import com.bazaarvoice.emodb.kafka.KafkaCluster;
import com.bazaarvoice.emodb.kafka.Topic;
import com.bazaarvoice.megabus.refproducer.MegabusRefProducerConfiguration;
import com.bazaarvoice.megabus.refproducer.MegabusRefProducerManager;
import com.bazaarvoice.megabus.refproducer.MegabusRefSubscriptionMonitorManager;
import com.bazaarvoice.megabus.refproducer.NumRefPartitions;
import com.bazaarvoice.megabus.resolver.DocumentResolverManager;
import com.bazaarvoice.megabus.resolver.MegabusRefResolver;
import com.bazaarvoice.megabus.resolver.MissingRefDelayProcessor;
import com.google.common.collect.ImmutableMap;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import java.time.Duration;
import org.apache.kafka.common.config.TopicConfig;

public class MegabusModule extends PrivateModule {

    private final int REF_PARTITIONS = 4;

    private final EmoServiceMode _serviceMode;

    public MegabusModule(EmoServiceMode serviceMode) {
        _serviceMode = serviceMode;
    }

    @Override
    protected void configure() {
        bind(Integer.class).annotatedWith(NumRefPartitions.class).toInstance(REF_PARTITIONS);

        bind(RateLimitedLogFactory.class).to(DefaultRateLimitedLogFactory.class).asEagerSingleton();

        bind(MegabusRefProducerManager.class).asEagerSingleton();
        bind(MegabusRefResolver.class).asEagerSingleton();
        bind(MissingRefDelayProcessor.class).asEagerSingleton();
        bind(DocumentResolverManager.class).asEagerSingleton();
        bind(MegabusBootWorkflowManager.class).asEagerSingleton();
        bind(MegabusRefSubscriptionMonitorManager.class).asEagerSingleton();
    }

    @Provides
    @Singleton
    @MegabusRefTopic
    Topic provideMegabusRefTopic(MegabusConfiguration megabusConfiguration, KafkaCluster kafkaCluster) {

        kafkaCluster.createTopicIfNotExists(megabusConfiguration.getMegabusRefTopic(),
                ImmutableMap.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE,
                        TopicConfig.RETENTION_MS_CONFIG, Long.toString(Duration.ofDays(30).toMillis()),
                        TopicConfig.COMPRESSION_TYPE_CONFIG, "zstd"));
        return megabusConfiguration.getMegabusRefTopic();
    }

    @Provides
    @Singleton
    @MegabusTopic
    Topic provideMegabusTopic(MegabusConfiguration megabusConfiguration, KafkaCluster kafkaCluster) {
        kafkaCluster.createTopicIfNotExists(megabusConfiguration.getMegabusTopic(),
                ImmutableMap.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT,
                        TopicConfig.DELETE_RETENTION_MS_CONFIG, Long.toString(Duration.ofDays(14).toMillis()),
                        TopicConfig.COMPRESSION_TYPE_CONFIG, "zstd"));
        return megabusConfiguration.getMegabusTopic();
    }

    @Provides
    @Singleton
    @MissingRefTopic
    Topic provideMissingRefTopic(MegabusConfiguration megabusConfiguration, KafkaCluster kafkaCluster) {
        kafkaCluster.createTopicIfNotExists(megabusConfiguration.getMissingRefTopic(),
                ImmutableMap.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE,
                        TopicConfig.RETENTION_MS_CONFIG, Long.toString(Duration.ofDays(30).toMillis()),
                        TopicConfig.COMPRESSION_TYPE_CONFIG, "zstd"));
        return megabusConfiguration.getMissingRefTopic();
    }

    @Provides
    @Singleton
    @RetryRefTopic
    Topic provideRetryRefTopic(MegabusConfiguration megabusConfiguration, KafkaCluster kafkaCluster) {
        kafkaCluster.createTopicIfNotExists(megabusConfiguration.getRetryRefTopic(),
                ImmutableMap.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE,
                        TopicConfig.RETENTION_MS_CONFIG, Long.toString(Duration.ofDays(30).toMillis()),
                        TopicConfig.COMPRESSION_TYPE_CONFIG, "zstd"));
        return megabusConfiguration.getRetryRefTopic();
    }

    @Provides
    @Singleton
    MegabusRefProducerConfiguration provideMegabusRefProducerConfiguration(MegabusConfiguration megabusConfiguration) {
        return megabusConfiguration.getRefProducerConfiguration();
    }

}
