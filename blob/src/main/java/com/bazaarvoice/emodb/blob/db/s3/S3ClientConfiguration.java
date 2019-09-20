package com.bazaarvoice.emodb.blob.db.s3;

public class S3ClientConfiguration {

    private EndpointConfiguration endpointConfiguration;

    public EndpointConfiguration getEndpointConfiguration() {
        return endpointConfiguration;
    }

    public void setEndpointConfiguration(final EndpointConfiguration endpointConfiguration) {
        this.endpointConfiguration = endpointConfiguration;
    }

    public static final class EndpointConfiguration {
        // the service endpoint either with or without the protocol (e.g. https://sns.us-west-1.amazonaws.com or sns.us-west-1.amazonaws.com)
        private String serviceEndpoint;
        // signingRegion the region to use for SigV4 signing of requests (e.g. us-west-1)
        private String signingRegion;

        public void setServiceEndpoint(final String serviceEndpoint) {
            this.serviceEndpoint = serviceEndpoint;
        }

        public void setSigningRegion(final String signingRegion) {
            this.signingRegion = signingRegion;
        }

        public String getServiceEndpoint() {
            return serviceEndpoint;
        }

        public String getSigningRegion() {
            return signingRegion;
        }
    }
}
