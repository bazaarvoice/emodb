package com.bazaarvoice.emodb.kafka;

import com.bazaarvoice.emodb.kafka.health.KafkaAdminHealthCheck;
import com.bazaarvoice.emodb.kafka.health.KafkaProducerHealthCheck;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;

import javax.annotation.Nullable;
import java.util.Properties;

public class KafkaModule extends PrivateModule {
    @Override
    protected void configure() {
        bind(KafkaCluster.class).to(DefaultKafkaCluster.class).asEagerSingleton();
        expose(KafkaCluster.class);
        bind(KafkaAdminHealthCheck.class).asEagerSingleton();
        bind(KafkaProducerHealthCheck.class).asEagerSingleton();
    }

    @Provides
    @Singleton
    @BootstrapServers
    String provideBootstrapServers(KafkaConfiguration kafkaConfiguration) {
        return kafkaConfiguration.getBootstrapServers();
    }


    @Provides
    @Singleton
    AdminClient provideAdminClient(@BootstrapServers String bootstrapServers, @Nullable SaslConfiguration saslConfiguration) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        if (null != saslConfiguration) {
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SaslConfiguration.PROTOCOL);
            properties.put(SaslConfigs.SASL_MECHANISM,SaslConfiguration.SASL_MECHANISM);
            properties.put(SaslConfigs.SASL_JAAS_CONFIG, saslConfiguration.getJaasConfig());
        }

        return AdminClient.create(properties);
    }

    @Provides
    @Singleton
    KafkaProducerConfiguration provideKafkaProducerConfiguration(KafkaConfiguration kafkaConfiguration) {
        return kafkaConfiguration.getKafkaProducerConfiguration();
    }

    @Nullable
    @Provides
    @Singleton
    SaslConfiguration provideSaslConfiguration(KafkaConfiguration kafkaConfiguration) {
        return kafkaConfiguration.getSaslConfiguration();
    }
}
