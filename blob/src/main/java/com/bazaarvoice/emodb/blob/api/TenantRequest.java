package com.bazaarvoice.emodb.blob.api;

public class TenantRequest {

    private String tenantName;

    public TenantRequest(String tenantName) {
        this.tenantName = tenantName;
    }

    public String getTenantName() {
        return tenantName;
    }

    public void setTenantName(String tenantName) {
        this.tenantName = tenantName;
    }

    @Override
    public String toString() {
        return "{\"tenantName\":\"" + tenantName + "\"}";
    }
}
