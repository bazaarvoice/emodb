package com.bazaarvoice.emodb.blob.api;

public class BlobAttributes {
    private String id;
    private String timestamp;
    private long length;
    private String md5;
    private String sha1;
    private Attributes attributes;

    public BlobAttributes(String id, String timestamp, long length, String md5, String sha1, Attributes attributes) {
        this.id = id;
        this.timestamp = timestamp;
        this.length = length;
        this.md5 = md5;
        this.sha1 = sha1;
        this.attributes = attributes;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public String getMd5() {
        return md5;
    }

    public void setMd5(String md5) {
        this.md5 = md5;
    }

    public String getSha1() {
        return sha1;
    }

    public void setSha1(String sha1) {
        this.sha1 = sha1;
    }

    public Attributes getAttributes() {
        return attributes;
    }

    public void setAttributes(Attributes attributes) {
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "{" +
                "\"id\": \"" + id + "\"" +
                ", \"timestamp\": \"" + timestamp + "\"" +
                ", \"length\": \"" + length + "\"" +
                ", \"md5\": \"" + md5 + "\"" +
                ", \"sha1\": \"" + sha1 + "\"" +
                ", \"attributes\": " + attributes +
                "}";
    }
}
