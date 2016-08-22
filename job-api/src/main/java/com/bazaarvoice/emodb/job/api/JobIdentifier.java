package com.bazaarvoice.emodb.job.api;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Longs;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Identifier for a job.  Creates a string ID which contains the job type name encoded within.  This allows for
 * an extra layer of validation when making requests to {@link JobService}.
 */
public class JobIdentifier<Q, R> {

    // The identifier for use in the public API
    private final String _id;
    // The name of the job type associated with this identifier
    private final JobType<Q, R> _jobType;

    private JobIdentifier(String id, JobType<Q, R> jobType) {
        _id = id;
        _jobType = jobType;
    }

    public static <Q, R> JobIdentifier<Q, R> createNew(JobType<Q, R> jobType) {
        checkNotNull(jobType, "jobType");
        return new JobIdentifier<>(toId(jobType.getName(), UUID.randomUUID()), jobType);
    }

    public static <Q, R> JobIdentifier<Q, R> fromString(String id, JobType<Q, R> jobType) {
        checkNotNull(id, "id");
        checkNotNull(jobType, "jobType");
        checkArgument(jobType.getName().equals(getJobTypeNameFromId(id)), "Inconsistent type");

        return new JobIdentifier<>(id, jobType);
    }

    public JobType<Q, R> getJobType() {
        return _jobType;
    }

    public static String getJobTypeNameFromId(String id) {
        byte[] bytes = BaseEncoding.base32().omitPadding().decode(id);
        // Skip the first 16 bytes which are the unique identifier
        return new String(bytes, 16, bytes.length-16, Charsets.UTF_8);
    }

    private static String toId(String type, UUID uuid) {
        Writer idWriter = new StringWriter();
        try (OutputStream buildStream = BaseEncoding.base32().omitPadding().encodingStream(idWriter)) {
            buildStream.write(Longs.toByteArray(uuid.getMostSignificantBits()));
            buildStream.write(Longs.toByteArray(uuid.getLeastSignificantBits()));
            buildStream.write(type.getBytes(Charsets.UTF_8));
        } catch (IOException e) {
           throw Throwables.propagate(e);  // Won't happen, StringWriter won't throw IOException
        }

        return idWriter.toString();
    }

    @JsonValue
    public String toString() {
        return _id;
    }

    public boolean equals(Object other) {
        return other == this ||
                (other instanceof JobIdentifier && _id.equals(((JobIdentifier) other)._id));
    }

    public int hashCode() {
        return _id.hashCode();
    }
}
