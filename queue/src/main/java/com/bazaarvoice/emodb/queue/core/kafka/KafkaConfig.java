package com.bazaarvoice.emodb.queue.core.kafka;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagement;

import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterRequest;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterResult;
import com.bazaarvoice.emodb.auth.proxy.Credential;
import org.apache.http.client.CredentialsProvider;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class KafkaConfig {
    private static String bootstrapServers="b-3.certemodbqueueqa.2ndps1.c4.kafka.us-east-1.amazonaws.com:9098,b-2.certemodbqueueqa.2ndps1.c4.kafka.us-east-1.amazonaws.com:9098,b-1.certemodbqueueqa.2ndps1.c4.kafka.us-east-1.amazonaws.com:9098";
    //private static final AWSSimpleSystemsManagement ssmClient = AWSSimpleSystemsManagementClientBuilder.standard().withCredentials( new ProfileCredentialsProvider("emodb-nexus-qa")).build();


//    static String batchSizeConfig = getParameterValue("/kafka/batchSize");
//    static String retriesConfig = getParameterValue("/kafka/retries");
//    static String lingerMsConfig = getParameterValue("/kafka/lingerMs");
//    static String bootstrapServersConfig = getParameterValue("/kafka/bootstrapServers");
//    private static String getParameterValue(String parameterName) {
//        GetParameterRequest request = new GetParameterRequest().withName(parameterName).withWithDecryption(true);
//        GetParameterResult result = ssmClient.getParameter(request);
//        return result.getParameter().getValue();
//    }
//        public static Properties getProducerProps () {
//            Properties producerProps = new Properties();
//            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersConfig);
//            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//            producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
//            producerProps.put(ProducerConfig.RETRIES_CONFIG, retriesConfig);
//            producerProps.put(ProducerConfig.LINGER_MS_CONFIG, lingerMsConfig);
//            producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSizeConfig);
//            producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
//            return producerProps;
//        }
public static Properties getProducerProps () {
    Properties producerProps = new Properties();

    // Enable IAM authentication
    producerProps.put("security.protocol", "SASL_SSL");
    producerProps.put("sasl.mechanism", "AWS_MSK_IAM");
    producerProps.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
    producerProps.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");

    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
    producerProps.put(ProducerConfig.RETRIES_CONFIG, 3);
    producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 5);
    producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
    return producerProps;
}

        public static Properties getAdminProps () {
            Properties adminProps = new Properties();
            adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            // Enable IAM authentication
            adminProps.put("security.protocol", "SASL_SSL");
            adminProps.put("sasl.mechanism", "AWS_MSK_IAM");
            adminProps.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
            adminProps.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
            return adminProps;
        }
}

