package com.bazaarvoice.megabus;

import com.bazaarvoice.emodb.common.dropwizard.log.DefaultRateLimitedLogFactory;
import com.bazaarvoice.emodb.common.dropwizard.log.RateLimitedLogFactory;
import com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode;
import com.bazaarvoice.emodb.kafka.Topic;
import com.bazaarvoice.megabus.streams.DocumentResolverManager;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

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
        bind(DocumentResolverManager.class).asEagerSingleton();
        bind(MegabusBootWorkflowManager.class).asEagerSingleton();
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
