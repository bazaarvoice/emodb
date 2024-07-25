package com.bazaarvoice.emodb.blob.config;

public class PlatformClient {

    private String table;
    private String clientName;

    public PlatformClient(String table, String clientName) {
        this.table = table;
        this.clientName = clientName;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getClientName() {
        return clientName;
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    @Override
    public String toString() {
        return "{" +
                "\"table\": \"" + table + "\"" +
                ", \"clientName\": \"" + clientName + "\"" +
                "}";
    }
}
