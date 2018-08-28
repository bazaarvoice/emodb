package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.web.jersey.params.SecurityExceptionMapper;
import com.google.common.collect.ImmutableList;

public class ExceptionMappers {

    public static Iterable<Object> getMappers() {
        return ImmutableList.<Object>of(
                new IllegalArgumentExceptionMapper(),
                new BlobNotFoundExceptionMapper(),
                new FacadeExistsExceptionMapper(),
                new RangeNotSatisfiableExceptionMapper(),
                new ReadOnlyQueueExceptionMapper(),
                new TableExistsExceptionMapper(),
                new UnknownFacadeExceptionMapper(),
                new UnknownSubscriptionExceptionMapper(),
                new UnknownTableExceptionMapper(),
                new UnknownPlacementExceptionMapper(),
                new SecurityExceptionMapper(),
                new UnknownQueueMoveExceptionMapper(),
                new UnknownDatabusMoveExceptionMapper(),
                new UnknownDatabusReplayExceptionMapper(),
                new JsonStreamProcessingExceptionMapper(),
                new StashNotAvailableExceptionMapper(),
                new DeltaSizeLimitExceptionMapper(),
                new AuditSizeLimitExceptionMapper(),
                new UnauthorizedSubscriptionExceptionMapper(),
                new ServiceUnavailableExceptionMapper(),
                new EmoRoleNotFoundExceptionMapper(),
                new EmoRoleExistsExceptionMapper(),
                new EmoApiKeyNotFoundExceptionMapper(),
                new EmoApiKeyExistsExceptionMapper(),
                new InvalidEmoPermissionExceptionMapper(),
                new InsufficientRolePermissionExceptionMapper(),
                new InvalidStashRequestExceptionMapper(),
                new UnsupportedOperationExceptionMapper());
    }

    public static Iterable<Class> getMapperTypes() {
        return ImmutableList.<Class>of(
                UncheckedExecutionExceptionMapper.class,
                PartitionForwardingExceptionMapper.class);
    }
}
