package com.bazaarvoice.emodb.web.resources.blob.messageQueue;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SQSServiceFactory {
    public MessagingService createSQSService() {
        ObjectMapper objectMapper = new ObjectMapper();
        AmazonSQS sqsClient = AmazonSQSClientBuilder.standard().build();
        return new SQSService("abqueue", objectMapper, sqsClient);
    }
}

