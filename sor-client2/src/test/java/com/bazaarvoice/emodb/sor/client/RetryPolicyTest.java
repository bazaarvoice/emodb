package com.bazaarvoice.emodb.sor.client;

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
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

@Test
public class RetryPolicyTest {

    @Test
    public void testRetryPolicyWhenRetriableExceptionTrue() throws InterruptedException {

        URI endpoint = URI.create("http://test/endpoint");
        Client client = ClientBuilder.newClient();
        DataStoreClientFactory factory = DataStoreClientFactory.forClusterAndHttpClient(endpoint, client);

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
    public void testRedirectCall() {
        URI originalURI = URI.create("http://test.us.east.com/endpoint");
        URI redirectURIExpected = URI.create("http://test.eu.west.com/redirect/endpoint");

        Client client = ClientBuilder.newClient();
        DataStoreClientFactory factory = DataStoreClientFactory.forClusterAndHttpClient(originalURI, client);

        List list = Mockito.mock(List.class);
        EmoResponse response = Mockito.mock(EmoResponse.class);
        when(response.getStatus()).thenReturn(Response.Status.MOVED_PERMANENTLY.getStatusCode());
        when(response.getLocation()).thenReturn(redirectURIExpected);
        when(list.size()).thenThrow(new EmoClientException(response));


        AtomicReference<URI> redirectURIResult =null;
        RetryPolicy<Object> retryPolicy = factory.createRetryPolicy();
        Failsafe.with(retryPolicy)
                .onFailure(e -> {
                    System.out.println("Failed..");
                    Throwable ex = e.getException();
                    if (ex instanceof EmoClientException){
                        System.out.println("ex is instance of EmoClientException");
                        // The SoR returns a 301 response when we need to make this request against a different data center.
                        if(((EmoClientException) ex).getResponse().getStatus()
                                == Response.Status.MOVED_PERMANENTLY.getStatusCode()){
                            System.out.println("true");
                            redirectURIResult.set(((EmoClientException) ex).getResponse().getLocation());
                        }}})
                .get(() ->list.size());

        assertEquals(redirectURIResult, redirectURIExpected);
        System.out.println("Redirected request to another datacenter");

        when(list.size()).thenReturn(1);
        int result = Failsafe.with(retryPolicy)
                .get(() -> list.size());
        assertEquals(result, 1);
    }
}
