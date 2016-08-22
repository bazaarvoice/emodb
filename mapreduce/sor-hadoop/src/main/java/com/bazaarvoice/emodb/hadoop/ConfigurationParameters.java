package com.bazaarvoice.emodb.hadoop;

public class ConfigurationParameters {

    /** General parameters */

    // Optional configuration to set the split size
    public static final String SPLIT_SIZE = "com.bazaarvoice.emodb.mapreduce.splitsize";

    /** EmoFileSystem parameters for use with "emodb" scheme */

    // API Key for connecting to EmoDB
    public static final String EMO_API_KEY = "com.bazaarvoice.emodb.apiKey";
    // Optional parameter to set the ZooKeeper connection string when using host discovery.  Default is usually correct,
    // such as "zookeeper1:2181,zookeeper2:2181,zookeeper3:2181
    public static final String ZOOKEEPER_CONNECTION_STRING_PARAM = "com.bazaarvoice.emodb.discovery.zkConnectionString";
    // Optional parameter to explicitly set the hosts for service discovery.  Value should be a comma-delimited list of hosts.
    public static final String HOSTS_PARAM = "com.bazaarvoice.emodb.discovery.hosts";

    /** StashFileSystem parameters for use with "emostash" scheme */

    // Parameter to set the S3 region.  It is most efficient to use whichever region is most local to where the job is running.
    public static final String REGION_PARAM = "com.bazaarvoice.emodb.stash.s3.region";
    // Optional parameter for the S3 access key.  If not provided the default AWS credentials chain will be used.
    public static final String ACCESS_KEY_PARAM = "com.bazaarvoice.emodb.stash.s3.accessKey";
    // Optional parameter for the S3 secret key.  If not provided the default AWS credentials chain will be used.
    public static final String SECRET_KEY_PARAM = "com.bazaarvoice.emodb.stash.s3.secretKey";

}
