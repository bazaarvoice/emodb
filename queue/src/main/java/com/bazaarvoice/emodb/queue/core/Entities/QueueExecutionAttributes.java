package com.bazaarvoice.emodb.queue.core.Entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class QueueExecutionAttributes {

    private String queueName;
    private String queueType;
    private String topicName;
    private Integer queueThreshold;
    private Integer batchSize;
    private Integer interval;
    private String status;

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void setQueueType(String queueType) {
        this.queueType = queueType;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public void setQueueThreshold(int queueThreshold) {
        this.queueThreshold = queueThreshold;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

    public Integer getQueueThreshold() {
        return queueThreshold;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public Integer getInterval() {
        return interval;
    }

    public String getQueueName() {
        return queueName;
    }

    public String getQueueType() {
        return queueType;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getJsonPayload(QueueExecutionAttributes attributes) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(attributes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QueueExecutionAttributes)) return false;
        QueueExecutionAttributes that = (QueueExecutionAttributes) o;
        return Objects.equals(getQueueName(), that.getQueueName()) && Objects.equals(getQueueType(), that.getQueueType()) && Objects.equals(getTopicName(), that.getTopicName()) && Objects.equals(getQueueThreshold(), that.getQueueThreshold()) && Objects.equals(getBatchSize(), that.getBatchSize()) && Objects.equals(getInterval(), that.getInterval());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getQueueName(), getQueueType(), getTopicName(), getQueueThreshold(), getBatchSize(), getInterval());
    }

    @Override
    public String toString() {
        return "QueueExecutionAttributes{" +
                "queueName='" + queueName + '\'' +
                ", queueType='" + queueType + '\'' +
                ", topicName='" + topicName + '\'' +
                ", queueThreshold=" + queueThreshold +
                ", batchSize=" + batchSize +
                ", interval=" + interval +
                ", status='" + status + '\'' +
                '}';
    }
}

