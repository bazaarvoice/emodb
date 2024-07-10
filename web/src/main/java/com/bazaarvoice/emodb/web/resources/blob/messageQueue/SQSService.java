package com.bazaarvoice.emodb.web.resources.blob.messageQueue;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.util.HashMap;
import java.util.Map;

/**
 * Service class for interacting with Amazon SQS (Simple Queue Service).
 */
public class SQSService implements MessagingService {
    private static final Logger _log = LoggerFactory.getLogger(SQSService.class);

    private final AmazonSQS sqs;
    private final String queueUrl;
    private final ObjectMapper objectMapper;

    /**
     * Constructor for SQSService.
     *
     * @param queueName      The name of the SQS queue to send messages to.
     * @param objectMapper   ObjectMapper for converting messages to JSON format.
     * @param sqs            AmazonSQS for sending messages
     */
    public SQSService(String queueName, ObjectMapper objectMapper, AmazonSQS sqs) {
        this.objectMapper = objectMapper;
        this.sqs = sqs;
        this.queueUrl = sqs.getQueueUrl(queueName).getQueueUrl();
    }

    @Override
    public void sendPutRequestSQS(String table, String blobId, byte[] byteArray, Map<String, String> attributes, String requestUrl) {
        Map<String, Object> messageMap = new HashMap<>();
        messageMap.put("method", "PUT_TABLE_BLOBID");
        messageMap.put("tenantName", "datastorage");
        messageMap.put("requestUrl", requestUrl);
        messageMap.put("table", table);
        messageMap.put("blobId", blobId);
        messageMap.put("attributes", attributes);

        // Logging the length of the byte array
        _log.debug("Byte array length: {}", byteArray.length);

        // Convert byte array to base64 string
        String base64Data = DatatypeConverter.printBase64Binary(byteArray);
        messageMap.put("data", base64Data);
        _log.debug("Sending PUT request to SQS. Table: {}, BlobId: {}, RequestUrl: {}", table, blobId, requestUrl);
        sendMessageSQS(messageMap);
    }

    @Override
    public void sendDeleteRequestSQS(String table, String blobId) {
        Map<String, Object> messageMap = new HashMap<>();
        messageMap.put("method", "DELETE_BLOB");
        messageMap.put("table", table);
        messageMap.put("blobId", blobId);
        sendMessageSQS(messageMap);
    }

    @Override
    public void sendCreateTableSQS(String table, TableOptions options, Map<String, String> attributes, Audit audit) {
        Map<String, Object> messageMap = new HashMap<>();
        messageMap.put("method", "CREATE_TABLE");
        messageMap.put("table", table);
        messageMap.put("options", options);
        messageMap.put("attributes", attributes);
        messageMap.put("audit", audit);
        sendMessageSQS(messageMap);
    }

    @Override
    public void sendDeleteTableSQS(String table, Audit audit) {
        Map<String, Object> messageMap = new HashMap<>();
        messageMap.put("method", "DELETE_TABLE");
        messageMap.put("table", table);
        messageMap.put("audit", audit);
        sendMessageSQS(messageMap);
    }

    @Override
    public void purgeTableSQS(String table, Audit audit) {
        Map<String, Object> messageMap = new HashMap<>();
        messageMap.put("method", "PURGE_TABLE");
        messageMap.put("table", table);
        messageMap.put("audit", audit);
        sendMessageSQS(messageMap);
    }

    @Override
    public void putTableAttributesSQS(String table, Map<String, String> attributes, Audit audit) {
        Map<String, Object> messageMap = new HashMap<>();
        messageMap.put("method", "SET_TABLE_ATTRIBUTE");
        messageMap.put("table", table);
        messageMap.put("attributes", attributes);
        messageMap.put("audit", audit);
        sendMessageSQS(messageMap);
    }

    private void sendMessageSQS(Map<String, Object> messageMap) {
        try {
            String messageBody = objectMapper.writeValueAsString(messageMap);
            String messageGroupId = "blob";
            SendMessageRequest sendMessageRequest = new SendMessageRequest()
                    .withQueueUrl(queueUrl)
                    .withMessageBody(messageBody)
                    .withMessageGroupId(messageGroupId);
            SendMessageResult result = sqs.sendMessage(sendMessageRequest);
            _log.info("Message sent successfully to SQS. Message ID: {}", result.getMessageId());
        } catch (JsonProcessingException e) {
            _log.error("Error converting message to JSON: {}", e.getMessage());
            throw new SQSMessageException("Failed to convert message to JSON", e);
        } catch (AmazonServiceException e) {
            _log.error("AmazonServiceException: {}", e.getMessage());
            throw new SQSMessageException("AWS service error occurred while sending message to SQS", e);
        } catch (AmazonClientException e) {
            _log.error("AmazonClientException: {}", e.getMessage());
            throw new SQSMessageException("Client error occurred while sending message to SQS", e);
        } catch (Exception e) {
            _log.error("Unexpected error occurred: {}", e.getMessage(), e);
            throw new SQSMessageException("Unexpected error occurred while sending message to SQS", e);
        }
    }
}


