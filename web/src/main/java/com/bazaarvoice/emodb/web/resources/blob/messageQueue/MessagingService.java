package com.bazaarvoice.emodb.web.resources.blob.messageQueue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.TableOptions;

import java.io.IOException;
import java.util.Map;

/**
 * Interface for interacting with a messaging service.
 */
public interface MessagingService {
    void sendPutRequestSQS(String table, String blobId, byte[] byteArray, Map<String, String> attributes, String requestUrl) throws IOException;
    void sendDeleteRequestSQS(String table, String blobId) throws IOException;
    void sendCreateTableSQS(String table, TableOptions options, Map<String, String> attributes, Audit audit) throws JsonProcessingException;
    void sendDeleteTableSQS(String table, Audit audit) throws IOException;
    void purgeTableSQS(String table, Audit audit) throws IOException;
    void putTableAttributesSQS(String table, Map<String, String> attributes, Audit audit) throws JsonProcessingException;
}


