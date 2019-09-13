package com.bazaarvoice.emodb.kafka;

public class Constants {
    public static final String ACKS_CONFIG = "all";
    public static final int RETRIES_CONFIG = 0;
    public static final String PRODUCER_COMPRESSION_TYPE = "zstd";
    public static final int MAX_REQUEST_SIZE =  15 * 1024 * 1024; // 15MB
}
