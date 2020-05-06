package com.bazaarvoice.emodb.blob.db.s3.config;

import java.time.Duration;

public class S3ClientConfiguration {

    private EndpointConfiguration endpointConfiguration;
    private RateLimitConfiguration rateLimitConfiguration = new RateLimitConfiguration();

    public EndpointConfiguration getEndpointConfiguration() {
        return endpointConfiguration;
    }

    public void setEndpointConfiguration(final EndpointConfiguration endpointConfiguration) {
        this.endpointConfiguration = endpointConfiguration;
    }

    public RateLimitConfiguration getRateLimitConfiguration() {
        return rateLimitConfiguration;
    }

    public void setRateLimitConfiguration(final RateLimitConfiguration rateLimitConfiguration) {
        this.rateLimitConfiguration = rateLimitConfiguration;
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

    public static class RateLimitConfiguration {
        private Duration decreaseCooldown = Duration.ofSeconds(2);
        private Duration increaseCooldown = Duration.ofSeconds(5);

        private int maxAttempts = 5;
        private double maxRateLimit = 50;
        private double minRateLimit = 1;

        public Duration getDecreaseCooldown() {
            return decreaseCooldown;
        }

        public void setDecreaseCooldown(final Duration decreaseCooldown) {
            this.decreaseCooldown = decreaseCooldown;
        }

        public Duration getIncreaseCooldown() {
            return increaseCooldown;
        }

        public void setIncreaseCooldown(final Duration increaseCooldown) {
            this.increaseCooldown = increaseCooldown;
        }

        public int getMaxAttempts() {
            return maxAttempts;
        }

        public void setMaxAttempts(final int maxAttempts) {
            this.maxAttempts = maxAttempts;
        }

        public double getMaxRateLimit() {
            return maxRateLimit;
        }

        public void setMaxRateLimit(final double maxRateLimit) {
            this.maxRateLimit = maxRateLimit;
        }

        public double getMinRateLimit() {
            return minRateLimit;
        }

        public void setMinRateLimit(final double minRateLimit) {
            this.minRateLimit = minRateLimit;
        }
    }
}
