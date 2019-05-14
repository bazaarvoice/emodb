package com.bazaarvoice.megabus;

import com.bazaarvoice.emodb.common.dropwizard.log.DefaultRateLimitedLogFactory;
import com.bazaarvoice.emodb.common.dropwizard.log.RateLimitedLogFactory;
import com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode;
import com.bazaarvoice.megabus.streams.DocumentResolverManager;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

public class MegabusModule extends PrivateModule {

    private final EmoServiceMode _serviceMode;

    public MegabusModule(EmoServiceMode serviceMode) {
        _serviceMode = serviceMode;
    }

    @Override
    protected void configure() {
        bind(RateLimitedLogFactory.class).to(DefaultRateLimitedLogFactory.class).asEagerSingleton();
        bind(MegabusRefProducerManager.class).asEagerSingleton();
        bind(MegabusRefResolver.class).asEagerSingleton();
        bind(BootStatusDAO.class).to(TimerBootStatusDAO.class).asEagerSingleton();
        bind(DocumentResolverManager.class).asEagerSingleton();
    }

    @Provides
    @Singleton
    @BootstrapServers
    String provideBootstrapServers(MegabusConfiguration megabusConfiguration) {
        return megabusConfiguration.getKafkaBootstrapServers();
    }

    @Provides
    @Singleton
    Properties provideKafkaProperties(@BootstrapServers String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return properties;
    }

    @Provides
    @Singleton
    AdminClient provideAdminClient(Properties kafkaProperties) {
        return AdminClient.create(kafkaProperties);
    }

    @Provides
    @Singleton
    @MegabusRefTopic
    Topic provideMegabusRefTopic(MegabusConfiguration megabusConfiguration) {
        return megabusConfiguration.getMegabusRefTopic();
    }

    @Provides
    @Singleton
    @MegabusTopic
    Topic provideMegabusTopic(MegabusConfiguration megabusConfiguration) {
        return megabusConfiguration.getMegabusTopic();
    }

}
