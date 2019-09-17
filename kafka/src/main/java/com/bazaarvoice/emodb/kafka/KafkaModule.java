package com.bazaarvoice.emodb.kafka;

import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SslConfigs;

import javax.annotation.Nullable;
import java.util.Properties;

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
    AdminClient provideAdminClient(@BootstrapServers String bootstrapServers, @Nullable SslConfiguration sslConfiguration) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        if (null != sslConfiguration) {
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SslConfiguration.PROTOCOL);

            properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslConfiguration.getTrustStoreLocation());
            properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslConfiguration.getTrustStorePassword());

            properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslConfiguration.getKeyStoreLocation());
            properties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslConfiguration.getKeyStorePassword());
            properties.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, sslConfiguration.getKeyPassword());
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
    SslConfiguration provideSslConfiguration(KafkaConfiguration kafkaConfiguration) {
        return kafkaConfiguration.getSslConfiguration();
    }
}
