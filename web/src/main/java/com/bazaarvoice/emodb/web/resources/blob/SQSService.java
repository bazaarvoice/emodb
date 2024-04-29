package com.bazaarvoice.emodb.web.resources.blob;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SQSService {
    private static final Logger _log = LoggerFactory.getLogger(SQSService.class);

    private final AmazonSQS sqs;
    private final String queueUrl;
    private final ObjectMapper objectMapper;

    public SQSService( String queueName, ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.sqs = AmazonSQSClientBuilder.standard()
//                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, "us-east-1"))
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .build();
        this.queueUrl = sqs.getQueueUrl(queueName).getQueueUrl();
    }



    public void sendPutRequestToSQS(String table, String blobId,byte[] byteArray , Map<String, String> attributes) throws IOException {
        Map<String, Object> messageMap = new HashMap<>();
        messageMap.put("method", "PUT_TABLE_BLOBID");
        messageMap.put("table", table);
        messageMap.put("blobId", blobId);

        // Logging the length of the byte array
        _log.info("Byte array length: {}", byteArray.length);

        // Convert byte array to base64 string
        String base64Data = DatatypeConverter.printBase64Binary(byteArray);
        messageMap.put("data", base64Data);

        // Logging the base64 string
        _log.info("Base64 data: {}", base64Data);
        messageMap.put("attributes", attributes);
        String messageBody = objectMapper.writeValueAsString(messageMap);
        sqs.sendMessage(new SendMessageRequest(queueUrl, messageBody));
    }

    public void sendDeleteRequestToSQS(String table, String blobId) throws IOException {
        Map<String, Object> messageMap = new HashMap<>();
        messageMap.put("method", "DELETE");
        messageMap.put("table", table);
        messageMap.put("blobId", blobId);
        String messageBody = objectMapper.writeValueAsString(messageMap);
        sqs.sendMessage(new SendMessageRequest(queueUrl, messageBody));
    }

    public void sendCreateTabletoSQS(String table, TableOptions options, Map<String, String> attributes, Audit audit) throws JsonProcessingException {

        Map<String, Object> messageMap = new HashMap<>();
        messageMap.put("method", "CREATE_TABLE");
        messageMap.put("table", table);
        messageMap.put("options", options);
        messageMap.put("attributes", attributes);
        messageMap.put("audit", audit);
        String messageBody = objectMapper.writeValueAsString(messageMap);
        sqs.sendMessage(new SendMessageRequest(queueUrl, messageBody));
    }

    public void sendDeleteTableSQS(String table, Audit audit) throws IOException{
        Map<String, Object> messageMap = new HashMap<>();
        messageMap.put("method", "DELETE_TABLE");
        messageMap.put("table",table);
        messageMap.put("audit",audit);
        String messageBody = objectMapper.writeValueAsString(messageMap);
        sqs.sendMessage(new SendMessageRequest(queueUrl, messageBody));

    }

    public void purgeTableSQS(String table, Audit audit) throws IOException{
        Map<String, Object> messageMap= new HashMap<>();
        messageMap.put("method","PURGE_TABLE");
        messageMap.put("table",table);
        messageMap.put("audit",audit);
        String messageBody = objectMapper.writeValueAsString(messageMap);
        sqs.sendMessage(new SendMessageRequest(queueUrl, messageBody));

    }
    public void putTableAttributesSQS(String table, Map<String,String> attributes, Audit audit) throws JsonProcessingException {
        Map<String, Object> messageMap= new HashMap<>();
        messageMap.put("method","SET_TABLE_ATTRIBUTE");
        messageMap.put("table",table);
        messageMap.put("attributes",attributes);
        messageMap.put("audit",audit);
        String messageBody = objectMapper.writeValueAsString(messageMap);
        sqs.sendMessage(new SendMessageRequest(queueUrl, messageBody));
    }
}

