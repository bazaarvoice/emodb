package com.bazaarvoice.emodb.kafka;

import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

public class KafkaModule extends PrivateModule {
    @Override
    protected void configure() {
        bind(KafkaCluster.class).to(DefaultKafkaCluster.class).asEagerSingleton();
        expose(KafkaCluster.class);
    }

    @Provides
    @Singleton
    @BootstrapServers
    String provideBootstrapServers(KafkaConfiguration kafkaConfiguration) {
        return kafkaConfiguration.getBootstrapServers();
    }


    @Provides
    @Singleton
    AdminClient provideAdminClient(@BootstrapServers String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(properties);
    }

    @Provides
    @Singleton
    KafkaProducerConfiguration provideKafkaProducerConfiguration(KafkaConfiguration kafkaConfiguration) {
        return kafkaConfiguration.getKafkaProducerConfiguration();
    }

}
