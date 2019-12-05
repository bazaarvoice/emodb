package test.integration.exceptions;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.blob.api.BlobNotFoundException;
import com.bazaarvoice.emodb.blob.api.RangeNotSatisfiableException;
import com.bazaarvoice.emodb.common.json.JsonStreamProcessingException;
import com.bazaarvoice.emodb.databus.api.UnknownReplayException;
import com.bazaarvoice.emodb.databus.api.UnknownSubscriptionException;
import com.bazaarvoice.emodb.sor.api.AuditSizeLimitException;
import com.bazaarvoice.emodb.sor.api.DeltaSizeLimitException;
import com.bazaarvoice.emodb.sor.api.FacadeExistsException;
import com.bazaarvoice.emodb.sor.api.StashNotAvailableException;
import com.bazaarvoice.emodb.sor.api.TableExistsException;
import com.bazaarvoice.emodb.sor.api.UnknownFacadeException;
import com.bazaarvoice.emodb.sor.api.UnknownPlacementException;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.sortedq.core.ReadOnlyQueueException;
import com.bazaarvoice.emodb.test.ResourceTest;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.dropwizard.testing.junit.ResourceTestRule;
import javax.ws.rs.core.Response;
import org.junit.Rule;
import org.junit.Test;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

/**
 * Unit test to verify that each mapped exception is returning the expected headers and content.
 */
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class ExceptionMapperJerseyTest extends ResourceTest {

    @Rule
    public ResourceTestRule _resourceTestRule = setupResourceTestRule(Collections.<Object>singletonList(new ExceptionResource()),
            ImmutableMap.of(
                    "unused" , new ApiKey("id0", ImmutableSet.of()),
                    "also-unused", new ApiKey("id1", ImmutableSet.<String>of())),
            ImmutableMultimap.of());

    @Test
    public void testIllegalArgumentException() {
        String actual = callException("IllegalArgumentException", 400, IllegalArgumentException.class, String.class);
        assertEquals("illegal-argument-message", actual);
    }

    @Test
    public void testBlobNotFoundException() {
        BlobNotFoundException actual = callException("BlobNotFoundException", 404, BlobNotFoundException.class, BlobNotFoundException.class);
        assertEquals("blob-message", actual.getMessage());
        assertEquals("blob-id", actual.getBlobId());
    }

    @Test
    public void testFacadeExistsException() {
        FacadeExistsException actual = callException("FacadeExistsException", 409, FacadeExistsException.class, FacadeExistsException.class);
        assertEquals("facade-message", actual.getMessage());
        assertEquals("facade-table", actual.getTable());
        assertEquals("facade-placement", actual.getPlacement());
    }

    @Test
    public void testRangeNotSatisfiableException() {
        RangeNotSatisfiableException actual = callException("RangeNotSatisfiableException", 416, RangeNotSatisfiableException.class, RangeNotSatisfiableException.class);
        assertEquals("range-message", actual.getMessage());
        assertEquals(1000, actual.getOffset());
        assertEquals(100, actual.getLength());
    }

    @Test
    public void testReadOnlyQueueException() {
        String actual = callException("ReadOnlyQueueException", 503, ReadOnlyQueueException.class, String.class);
        assertEquals("Server does not manage the specified resource at this time.", actual);
    }

    @Test
    public void testTableExistsException() {
        TableExistsException actual = callException("TableExistsException", 409, TableExistsException.class, TableExistsException.class);
        assertEquals("table-message", actual.getMessage());
        assertEquals("table-name", actual.getTable());
    }

    @Test
    public void testUnknownFacadeException() {
        UnknownFacadeException actual = callException("UnknownFacadeException", 404, UnknownFacadeException.class, UnknownFacadeException.class);
        assertEquals("facade-message", actual.getMessage());
        assertEquals("facade-name", actual.getFacade());
    }

    @Test
    public void testUnknownSubscriptionException() {
        UnknownSubscriptionException actual = callException("UnknownSubscriptionException", 404, UnknownSubscriptionException.class, UnknownSubscriptionException.class);
        assertEquals("subscription-message", actual.getMessage());
        assertEquals("subscription-name", actual.getSubscription());
    }

    @Test
    public void testUnknownTableException() {
        UnknownTableException actual = callException("UnknownTableException", 404, UnknownTableException.class, UnknownTableException.class);
        assertEquals("table-message", actual.getMessage());
        assertEquals("table-name", actual.getTable());
    }

    @Test
    public void testUnknownPlacementException() {
        UnknownPlacementException actual = callException("UnknownPlacementException", 404, UnknownPlacementException.class, UnknownPlacementException.class);
        // The message gets rewritten by the exception mapper to the following
        assertEquals("Table table-name is stored in a locally inaccessible placement: placement-name", actual.getMessage());
        assertEquals("placement-name", actual.getPlacement());
        assertEquals("table-name", actual.getTable());
    }

    @Test
    public void testSecurityException() {
        String actual = callException("SecurityException", 403, SecurityException.class, String.class);
        assertEquals("security-message", actual);
    }

    @Test
    public void testUnknownQueueMoveException() {
        com.bazaarvoice.emodb.queue.api.UnknownMoveException actual = callException(
                "UnknownQueueMoveException", 404, com.bazaarvoice.emodb.queue.api.UnknownMoveException.class,
                com.bazaarvoice.emodb.queue.api.UnknownMoveException.class);
        assertEquals("queue-name", actual.getId());
    }

    @Test
    public void testUnknownDatabusMoveException() {
        com.bazaarvoice.emodb.databus.api.UnknownMoveException actual = callException(
                "UnknownDatabusMoveException", 404, com.bazaarvoice.emodb.databus.api.UnknownMoveException.class,
                com.bazaarvoice.emodb.databus.api.UnknownMoveException.class);
        assertEquals("subscription-name", actual.getId());
    }

    @Test
    public void testUnknownDatabusReplayException() {
        UnknownReplayException actual = callException("UnknownDatabusReplayException", 404, UnknownReplayException.class, UnknownReplayException.class);
        assertEquals("subscription-name", actual.getId());
    }

    @Test
    public void testJsonStreamProcessingException() {
        String actual = callException("JsonStreamProcessingException", 400, JsonStreamProcessingException.class, String.class);
        assertEquals("json-message", actual);
    }

    @Test
    public void testStashNotAvailableException() {
        StashNotAvailableException actual = callException("StashNotAvailableException", 404, StashNotAvailableException.class, StashNotAvailableException.class);
        assertEquals("stash-message", actual.getMessage());
    }

    @Test
    public void testDeltaSizeLimitException() {
        DeltaSizeLimitException actual = callException("DeltaSizeLimitException", 400, DeltaSizeLimitException.class, DeltaSizeLimitException.class);
        assertEquals("size-message", actual.getMessage());
        assertEquals(1000, actual.getSize());
    }

    @Test
    public void testAuditSizeLimitException() {
        AuditSizeLimitException actual = callException("AuditSizeLimitException", 400, AuditSizeLimitException.class, AuditSizeLimitException.class);
        assertEquals("size-message", actual.getMessage());
        assertEquals(1000, actual.getSize());
    }

    @Test
    public void testUncheckedExecutionException() {
        String actual = callException("UncheckedExecutionException", 403, SecurityException.class, String.class);
        assertEquals("unchecked-source-message", actual);
    }

    private <T> T callException(String path, int expectedStatus, Class<? extends Exception> expectedException, Class<T> entityType) {
        Response response = _resourceTestRule.client()
                .target("/exception/" + path)
                .request()
                .get();

        assertEquals(expectedStatus, response.getStatus());
        assertEquals(expectedException.getName(), response.getHeaders().getFirst("X-BV-Exception"));

        if (String.class.equals(entityType)) {
            assertEquals(MediaType.TEXT_PLAIN_TYPE, response.getMediaType());
        } else {
            assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());
        }

        return response.readEntity(entityType);

    }

    @Path("/exception")
    @Produces(MediaType.APPLICATION_JSON)
    public static class ExceptionResource {

        @GET
        @Path("IllegalArgumentException")
        public String throwsIllegalArgumentException() {
            throw new IllegalArgumentException("illegal-argument-message");
        }

        @GET
        @Path("BlobNotFoundException")
        public String throwsBlobNotFoundException() {
            throw new BlobNotFoundException("blob-message", "blob-id");
        }

        @GET
        @Path("FacadeExistsException")
        public String throwsFacadeExistsException() {
            throw new FacadeExistsException("facade-message", "facade-table", "facade-placement");
        }

        @GET
        @Path("RangeNotSatisfiableException")
        public String throwsRangeNotSatisfiableException() {
            throw new RangeNotSatisfiableException("range-message", 1000, 100);
        }

        @GET
        @Path("ReadOnlyQueueException")
        public String throwsReadOnlyQueueException() {
            throw new ReadOnlyQueueException("read-only-queue-message");
        }

        @GET
        @Path("TableExistsException")
        public String throwsTableExistsException() {
            throw new TableExistsException("table-message", "table-name");
        }

        @GET
        @Path("UnknownFacadeException")
        public String throwsUnknownFacadeException() {
            throw new UnknownFacadeException("facade-message", "facade-name");
        }

        @GET
        @Path("UnknownSubscriptionException")
        public String throwsUnknownSubscriptionException() {
            throw new UnknownSubscriptionException("subscription-message", "subscription-name");
        }

        @GET
        @Path("UnknownTableException")
        public String throwsUnknownTableException() {
            throw new UnknownTableException("table-message", "table-name");
        }

        @GET
        @Path("UnknownPlacementException")
        public String throwsUnknownPlacementException() {
            throw new UnknownPlacementException("placement-message", "placement-name", "table-name");
        }

        @GET
        @Path("SecurityException")
        public String throwsSecurityException() {
            throw new SecurityException("security-message");
        }

        @GET
        @Path("UnknownQueueMoveException")
        public String throwsUnknownQueueMoveException() {
            throw new com.bazaarvoice.emodb.queue.api.UnknownMoveException("queue-name");
        }

        @GET
        @Path("UnknownDatabusMoveException")
        public String throwsUnknownDatabusMoveException() {
            throw new com.bazaarvoice.emodb.databus.api.UnknownMoveException("subscription-name");
        }

        @GET
        @Path("UnknownDatabusReplayException")
        public String throwsUnknownDatabusReplayException() {
            throw new UnknownReplayException("subscription-name");
        }

        @GET
        @Path("JsonStreamProcessingException")
        public String throwsJsonStreamProcessingException() {
            throw new JsonStreamProcessingException("json-message");
        }

        @GET
        @Path("StashNotAvailableException")
        public String throwsStashNotAvailableException() {
            throw new StashNotAvailableException("stash-message");
        }

        @GET
        @Path("DeltaSizeLimitException")
        public String throwsDeltaSizeLimitException() {
            throw new DeltaSizeLimitException("size-message", 1000);
        }

        @GET
        @Path("AuditSizeLimitException")
        public String throwsAuditSizeLimitException() {
            throw new AuditSizeLimitException("size-message", 1000);
        }

        @GET
        @Path("UncheckedExecutionException")
        public String throwsUncheckedExecutionException() {
            // Cause just needs to be any of the other mapped exceptions; SecurityException was chosen arbitrarily
            throw new UncheckedExecutionException(new SecurityException("unchecked-source-message"));
        }
    }
}
