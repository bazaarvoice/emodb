package com.bazaarvoice.megabus;

import com.bazaarvoice.emodb.common.dropwizard.log.DefaultRateLimitedLogFactory;
import com.bazaarvoice.emodb.common.dropwizard.log.RateLimitedLogFactory;
import com.bazaarvoice.emodb.kafka.KafkaCluster;
import com.bazaarvoice.emodb.kafka.Topic;
import com.bazaarvoice.megabus.guice.DelayProcessorConsumerGroup;
import com.bazaarvoice.megabus.guice.MegabusRefResolverService;
import com.bazaarvoice.megabus.guice.MegabusRefTopic;
import com.bazaarvoice.megabus.guice.MegabusTopic;
import com.bazaarvoice.megabus.guice.MissingRefDelayService;
import com.bazaarvoice.megabus.guice.MissingRefTopic;
import com.bazaarvoice.megabus.guice.RefResolverConsumerGroup;
import com.bazaarvoice.megabus.guice.RetryRefTopic;
import com.bazaarvoice.megabus.guice.TableEventProcessorService;
import com.bazaarvoice.megabus.guice.TableEventRegistrationService;
import com.bazaarvoice.megabus.refproducer.MegabusRefProducerConfiguration;
import com.bazaarvoice.megabus.refproducer.MegabusRefProducerManager;
import com.bazaarvoice.megabus.refproducer.MegabusRefSubscriptionMonitorManager;
import com.bazaarvoice.megabus.refproducer.NumRefPartitions;
import com.bazaarvoice.megabus.resolver.ResilientMegabusRefResolver;
import com.bazaarvoice.megabus.resolver.ResilientMissingRefDelayProcessor;
import com.bazaarvoice.megabus.tableevents.TableEventProcessorManager;
import com.bazaarvoice.megabus.tableevents.TableEventRegistrar;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.kafka.common.config.TopicConfig;

import java.time.Duration;

public class MegabusModule extends PrivateModule {

    /**
     * These values should not be changed without extreme caution. Changing them on a live system will likely not be
     * backward compatible
     */
    private static final int REF_PARTITIONS = 4;
    private static final String REF_TOPIC_RETENTION = Long.toString(Duration.ofDays(30).toMillis());
    private static final String MEGABUS_DELETE_RETENTION = Long.toString(Duration.ofDays(14).toMillis());
    private static final String COMPRESSION_CONFIGURATION = "producer";

    @Override
    protected void configure() {
        bind(Integer.class).annotatedWith(NumRefPartitions.class).toInstance(REF_PARTITIONS);

        bind(RateLimitedLogFactory.class).to(DefaultRateLimitedLogFactory.class).asEagerSingleton();

        bind(Service.class).annotatedWith(MegabusRefResolverService.class).to(ResilientMegabusRefResolver.class).asEagerSingleton();
        bind(Service.class).annotatedWith(MissingRefDelayService.class).to(ResilientMissingRefDelayProcessor.class).asEagerSingleton();
        bind(Service.class).annotatedWith(TableEventRegistrationService.class).to(TableEventRegistrar.class).asEagerSingleton();
        bind(Service.class).annotatedWith(TableEventProcessorService.class).to(TableEventProcessorManager.class).asEagerSingleton();
        bind(MegabusRefProducerManager.class).asEagerSingleton();

        bind(MegabusBootWorkflowManager.class).asEagerSingleton();
        bind(MegabusRefSubscriptionMonitorManager.class).asEagerSingleton();

        bind(MegabusSource.class).to(DefaultMegabusSource.class).asEagerSingleton();
        expose(MegabusSource.class);
        
        // TODO: bind this into the boot workflow to prevent race conditions
        bind(TableEventProcessorManager.class).asEagerSingleton();
        
        bind(MegabusRefSubscriptionMonitorManager.class).asEagerSingleton();
    }

    @Provides
    @Singleton
    @MegabusRefTopic
    Topic provideMegabusRefTopic(MegabusConfiguration megabusConfiguration, KafkaCluster kafkaCluster) {

        kafkaCluster.createTopicIfNotExists(megabusConfiguration.getMegabusRefTopic(),
                ImmutableMap.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE,
                        TopicConfig.RETENTION_MS_CONFIG, REF_TOPIC_RETENTION,
                        TopicConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_CONFIGURATION));
        return megabusConfiguration.getMegabusRefTopic();
    }

    @Provides
    @Singleton
    @MegabusTopic
    Topic provideMegabusTopic(MegabusConfiguration megabusConfiguration, KafkaCluster kafkaCluster) {
        kafkaCluster.createTopicIfNotExists(megabusConfiguration.getMegabusTopic(),
                ImmutableMap.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT,
                        TopicConfig.DELETE_RETENTION_MS_CONFIG, MEGABUS_DELETE_RETENTION,
                        TopicConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_CONFIGURATION));
        return megabusConfiguration.getMegabusTopic();
    }

    @Provides
    @Singleton
    @MissingRefTopic
    Topic provideMissingRefTopic(MegabusConfiguration megabusConfiguration, KafkaCluster kafkaCluster) {
        kafkaCluster.createTopicIfNotExists(megabusConfiguration.getMissingRefTopic(),
                ImmutableMap.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE,
                        TopicConfig.RETENTION_MS_CONFIG, REF_TOPIC_RETENTION,
                        TopicConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_CONFIGURATION));
        return megabusConfiguration.getMissingRefTopic();
    }

    @Provides
    @Singleton
    @RetryRefTopic
    Topic provideRetryRefTopic(MegabusConfiguration megabusConfiguration, KafkaCluster kafkaCluster) {
        kafkaCluster.createTopicIfNotExists(megabusConfiguration.getRetryRefTopic(),
                ImmutableMap.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE,
                        TopicConfig.RETENTION_MS_CONFIG, REF_TOPIC_RETENTION,
                        TopicConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_CONFIGURATION));
        return megabusConfiguration.getRetryRefTopic();
    }

    @Provides
    @Singleton
    MegabusRefProducerConfiguration provideMegabusRefProducerConfiguration(MegabusConfiguration megabusConfiguration) {
        return megabusConfiguration.getRefProducerConfiguration();
    }

    @Provides
    @Singleton
    @DelayProcessorConsumerGroup
    String provideDelayProcessorConsumerGroup(MegabusConfiguration megabusConfiguration) {
        return megabusConfiguration.getDelayProcessorConsumerGroupName();
    }

    @Provides
    @Singleton
    @RefResolverConsumerGroup
    String provideRefResolverConsumerGroup(MegabusConfiguration megabusConfiguration) {
        return megabusConfiguration.getRefResolverConsumerGroupName();
    }

}
