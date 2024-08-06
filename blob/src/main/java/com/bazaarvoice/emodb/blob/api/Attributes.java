package com.bazaarvoice.emodb.blob.api;

public class Attributes {
    private String client;
    private String contentType;
    private String legacyInternalId;
    private String platformclient;
    private String size;
    private String type;

    public Attributes(String client, String contentType, String legacyInternalId, String platformclient, String size, String type) {
        this.client = client;
        this.contentType = contentType;
        this.legacyInternalId = legacyInternalId;
        this.platformclient = platformclient;
        this.size = size;
        this.type = type;
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public String getLegacyInternalId() {
        return legacyInternalId;
    }

    public void setLegacyInternalId(String legacyInternalId) {
        this.legacyInternalId = legacyInternalId;
    }

    public String getPlatformclient() {
        return platformclient;
    }

    public void setPlatformclient(String platformclient) {
        this.platformclient = platformclient;
    }

    public String getSize() {
        return size;
    }

    public void setSize(String size) {
        this.size = size;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "{" +
                "\"client\": \"" + client + "\"" +
                ", \"contentType\": \"" + contentType + "\"" +
                ", \"legacyInternalId\": \"" + legacyInternalId + "\"" +
                ", \"platformclient\": \"" + platformclient + "\"" +
                ", \"size\": \"" + size + "\"" +
                ", \"type\": \"" + type + "\"" +
                "}";
    }
}
