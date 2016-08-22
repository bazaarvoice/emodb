package com.bazaarvoice.emodb.job.util;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.job.api.JobStatus;
import com.bazaarvoice.emodb.job.api.JobType;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.util.Types;

import java.lang.reflect.Type;

/**
 * Utility class for handling JobStatus object in a type-safe manner.
 */
abstract public class JobStatusUtil {

    /**
     * Optimization to perform narrowing of JobStatus objects by caching their TypeReferences based on the JobType.
     */
    private static final LoadingCache<JobType<?, ?>, TypeReference<JobStatus<?, ?>>> _jobStatusTypeReferences =
            CacheBuilder.newBuilder()
                    .build(new CacheLoader<JobType<?, ?>, TypeReference<JobStatus<?, ?>>>() {
                        @Override
                        public TypeReference<JobStatus<?, ?>> load(final JobType<?, ?> jobType)
                                throws Exception {
                            return new TypeReference<JobStatus<?, ?>>() {
                                private Type _type = Types.newParameterizedType(
                                        JobStatus.class, jobType.getRequestType(), jobType.getResultType());

                                @Override
                                public Type getType() {
                                    return _type;
                                }
                            };
                        }
                    });

    public static <Q, R> JobStatus<Q, R> narrow(Object object, JobType<Q, R> jobType) {
        return JsonHelper.convert(object, getTypeReferenceForJobType(jobType));
    }

    @SuppressWarnings("unchecked")
    private static <Q, R> TypeReference<JobStatus<Q, R>> getTypeReferenceForJobType(JobType<Q, R> jobType) {
        return (TypeReference) _jobStatusTypeReferences.getUnchecked(jobType);
    }
}
