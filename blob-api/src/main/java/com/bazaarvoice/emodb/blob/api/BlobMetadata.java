package com.bazaarvoice.emodb.blob.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.Date;
import java.util.Map;

@JsonDeserialize(as = DefaultBlobMetadata.class)
public interface BlobMetadata {

    /**
     * Returns the blob identifier.
     */
    String getId();

    /**
     * Returns the date/time when this blob was created.
     */
    Date getTimestamp();

    /**
     * Returns the size of the blob, in bytes.
     */
    long getLength();

    /**
     * Returns the MD5 hash of the blob bytes as a 32-character hex-encoded string.
     */
    String getMD5();

    /**
     * Returns the SHA1 hash of the blob bytes as a 40-character hex-encoded string.
     */
    String getSHA1();

    /**
     * Returns the attributes that were supplied with the first use of this blob.
     */
    Map<String, String> getAttributes();
}
