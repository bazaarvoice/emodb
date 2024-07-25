package com.bazaarvoice.emodb.blob.api;

public class UploadByteRequestBody {
    private String base64;
    private String tenantName;
    private BlobAttributes blobAttributes;

    public UploadByteRequestBody(String base64, String tenantName, BlobAttributes blobAttributes) {
        this.base64 = base64;
        this.tenantName = tenantName;
        this.blobAttributes = blobAttributes;
    }

    public String getBase64() {
        return base64;
    }

    public void setBase64(String base64) {
        this.base64 = base64;
    }

    public String getTenantName() {
        return tenantName;
    }

    public void setTenantName(String tenantName) {
        this.tenantName = tenantName;
    }

    public BlobAttributes getBlobAttributes() {
        return blobAttributes;
    }

    public void setBlobAttributes(BlobAttributes blobAttributes) {
        this.blobAttributes = blobAttributes;
    }

    @Override
    public String toString() {
        return "{" +
                "\"base64\": \"" + base64 + "\"" +
                ", \"tenantName\": \"" + tenantName + "\"" +
                ", \"blobAttributes\": " + blobAttributes +
                "}";
    }
}
