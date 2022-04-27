package com.bazaarvoice.emodb.common.jersey2;

import com.bazaarvoice.emodb.client2.EmoClientException;
import com.bazaarvoice.emodb.client2.EmoResponse;
import com.bazaarvoice.emodb.common.json.JsonStreamingEOFException;
import dev.failsafe.Execution;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import javax.ws.rs.core.Response;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class RetryPolicyTest {

    @Test
    public void failSafeAbortOnMethodCall() {

        List list = mock(List.class);
        when(list.size()).thenThrow(EmoClientException.class);

        final AtomicInteger result = new AtomicInteger(0);

        dev.failsafe.RetryPolicy<Object> retryPolicy = dev.failsafe.RetryPolicy.builder()
                .withBackoff(Duration.ofMillis(100), Duration.ofMillis(200))
                .withMaxRetries(10)
                .onRetry(e -> {
                    // Should not execute
                    System.out.println("Exception: " + e + "Retrying attempt" + result.incrementAndGet());
                })
                // Using .abortOn() to stop retry attempt on certain condition.
                .abortOn(e -> {
                    System.out.println(" Aborting execution due to " + e);
                    result.incrementAndGet();
                    return e instanceof EmoClientException;
                })
                .build();

        assertThrows(EmoClientException.class, () -> Failsafe.with(retryPolicy)
                .get(() -> list.size()));

        assertEquals(result.intValue(), 1);

    }

    @Test
    public void testRetryPolicyWhenRetriableExceptionTrue() throws InterruptedException {

        List list = mock(List.class);
        when(list.size()).thenThrow(JsonStreamingEOFException.class);

        RetryPolicy<Object> retryPolicy = com.bazaarvoice.emodb.common.jersey2.RetryPolicy.createRetryPolicy();

        Execution<Object> execution = Execution.of(retryPolicy);
        while (!execution.isComplete()) {

            try {
                execution.recordResult(list.size());
            } catch (RuntimeException e) {
                execution.recordFailure(e);
                // Wait before retrying
                Thread.sleep(execution.getDelay().toMillis());
            }
        }
        assertEquals(execution.getAttemptCount(), 4);
    }

    @Test
    public void testRetryPolicyWhenRetriableExceptionFalse() throws InterruptedException {


        List list = mock(List.class);
        EmoResponse _response = mock(EmoResponse.class);
        when(list.size()).thenThrow(new EmoClientException(_response));

        RetryPolicy<Object> retryPolicy = com.bazaarvoice.emodb.common.jersey2.RetryPolicy.createRetryPolicy();

        Execution<Object> execution = Execution.of(retryPolicy);

        while (!execution.isComplete()) {
            try {
                execution.recordResult(list.size());
            } catch (RuntimeException e) {
                execution.recordFailure(e);

                // Wait before retrying
                Thread.sleep(execution.getDelay().toMillis());
            }
        }
        assertEquals(execution.getAttemptCount(), 1);
    }

    @Test
    public void testRedirectCall() {
        URI redirectURIExpected = URI.create("http://test.eu.west.com/redirect/endpoint");

        List list = Mockito.mock(List.class);
        EmoResponse response = Mockito.mock(EmoResponse.class);
        when(response.getStatus()).thenReturn(Response.Status.MOVED_PERMANENTLY.getStatusCode());
        when(response.getLocation()).thenReturn(redirectURIExpected);
        when(list.size()).thenThrow(new EmoClientException(response));


        AtomicReference<URI> redirectURIActual = new AtomicReference<>();
        RetryPolicy<Object> retryPolicy = com.bazaarvoice.emodb.common.jersey2.RetryPolicy.createRetryPolicy();

        try {
            Failsafe.with(retryPolicy)
                    .onFailure(ex -> {
                        Throwable e = ex.getException();
                        if (e instanceof EmoClientException) {
                            if (((EmoClientException) e).getResponse().getStatus()
                                    == Response.Status.MOVED_PERMANENTLY.getStatusCode()) {
                                redirectURIActual.set(((EmoClientException) e).getResponse().getLocation());
                            } else {
                                throw e;
                            }
                        }
                    })
                    .run(() -> list.size());


        } catch (EmoClientException e) {
            assertEquals(redirectURIActual.get(), redirectURIExpected);
        }
    }
}
