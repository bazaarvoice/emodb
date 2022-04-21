package com.bazaarvoice.emodb.blob.jersey2.client;



import com.bazaarvoice.emodb.client2.EmoClientException;
import com.bazaarvoice.emodb.client2.EmoResponse;
import com.bazaarvoice.emodb.common.json.JsonStreamingEOFException;
import dev.failsafe.Execution;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


@Test
public class RetryLogicTest {

    @Test
    public void failSafeAbortOnMethodCall(){

        List list = Mockito.mock(List.class);
        when(list.size()).thenThrow(EmoClientException.class);

        final AtomicInteger result= new AtomicInteger(0);

        RetryPolicy<Object> retryPolicy = RetryPolicy.builder()
                .withBackoff(Duration.ofMillis(100), Duration.ofMillis(200))
                .withMaxRetries(10)
                .onRetry(e ->{
                    // Should not execute
                    System.out.println("Exception: "+e+ "Retrying attempt"+result.incrementAndGet());
                })
                // Using .abortOn() to stop retry attempt on certain condition.
                .abortOn(e ->{ System.out.println( " Aborting execution due to "+e);
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

        URI endpoint = URI.create("http://test/endpoint");
        Client client = ClientBuilder.newClient();
        BlobStoreJersey2ClientFactory factory = BlobStoreJersey2ClientFactory.forClusterAndHttpClient(client, endpoint);

        List list = Mockito.mock(List.class);
        when(list.size()).thenThrow(JsonStreamingEOFException.class);

        RetryPolicy<Object> retryPolicy = factory.createRetryPolicy();

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
        assertEquals(execution.getAttemptCount(),4);
    }

    @Test
    public void testRetryPolicyWhenRetriableExceptionFalse() throws InterruptedException {

        URI endpoint = URI.create("http://test/endpoint");
        Client client = ClientBuilder.newClient();
        BlobStoreJersey2ClientFactory factory = BlobStoreJersey2ClientFactory.forClusterAndHttpClient(client, endpoint);

        List list = Mockito.mock(List.class);
        EmoResponse _response = Mockito.mock(EmoResponse.class);
        when(list.size()).thenThrow(new EmoClientException(_response));

        RetryPolicy<Object> retryPolicy = factory.createRetryPolicy();

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
        assertEquals(execution.getAttemptCount(),1);
    }
}
