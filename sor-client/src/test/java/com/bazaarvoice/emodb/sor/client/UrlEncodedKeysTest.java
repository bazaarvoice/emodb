package com.bazaarvoice.emodb.sor.client;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.client.EmoResource;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.uuid.TimeUUIDs;
import org.mockito.ArgumentMatcher;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.ws.rs.core.MediaType;
import java.net.URI;
import java.net.URLEncoder;
import java.util.Objects;
import java.util.UUID;

import static java.lang.String.format;

public class UrlEncodedKeysTest {
    private EmoClient _client = Mockito.mock(EmoClient.class);

    @BeforeMethod
    public void setUp() {
        EmoResource resource = Mockito.mock(EmoResource.class);
        _client = Mockito.mock(EmoClient.class);

        Mockito.when(_client.resource(ArgumentMatchers.any(URI.class))).thenReturn(resource);
        Mockito.when(resource.accept(ArgumentMatchers.any(MediaType.class))).thenReturn(resource);
        Mockito.when(resource.type(ArgumentMatchers.any(MediaType.class))).thenReturn(resource);
        Mockito.when(resource.header(ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(resource);
    }

    @Test
    public void testGetEncodedKey() throws Exception {
        DataStoreClient dataStore = new DataStoreClient(new URI("http://test.server"), _client);
        String table = "test_get_encoded:table";
        String key = "near%20far";

        dataStore.get("api_key", table, key);

        Mockito.verify(_client, Mockito.times(1)).resource(ArgumentMatchers.argThat(getURIMatcher("test.server", table, key)));
    }

    @Test
    public void testGetTemplateKey() throws Exception {
        DataStoreClient dataStore = new DataStoreClient(new URI("http://test.server"), _client);
        String table = "test_get_template:table";
        String key = "/{value1}{value2}";

        dataStore.get("api_key", table, key);

        Mockito.verify(_client, Mockito.times(1)).resource(ArgumentMatchers.argThat(getURIMatcher("test.server", table, key)));
    }

    @Test
    public void testUpdateEncodedKey() throws Exception {
        DataStoreClient dataStore = new DataStoreClient(new URI("http://test.server"), _client);
        String table = "test_update_encoded:table";
        String key = "above%2fbelow";
        UUID changeId = TimeUUIDs.newUUID();

        dataStore.update("api_key", table, key, changeId,
                Deltas.mapBuilder()
                        .put("score", 10)
                        .put("technique", "awesome")
                        .build(),
                new AuditBuilder().setComment("judgement").build());

        Mockito.verify(_client, Mockito.times(1)).resource(ArgumentMatchers.argThat(getURIMatcher("test.server", table, key)));
    }

    @Test
    public void testUpdateTemplateKey() throws Exception {
        DataStoreClient dataStore = new DataStoreClient(new URI("http://test.server"), _client);
        String table = "test_update_encoded:table";
        String key = "{a{b}c}{}";
        UUID changeId = TimeUUIDs.newUUID();

        dataStore.update("api_key", table, key, changeId,
                Deltas.mapBuilder()
                        .put("score", 10)
                        .put("technique", "awesome")
                        .build(),
                new AuditBuilder().setComment("judgement").build());

        Mockito.verify(_client, Mockito.times(1)).resource(ArgumentMatchers.argThat(getURIMatcher("test.server", table, key)));
    }

    public ArgumentMatcher<URI> getURIMatcher(final String host, final String table, String key)
            throws Exception {
        final String encodedKey = URLEncoder.encode(key, "UTF-8");

        return new ArgumentMatcher<URI>() {
            @Override
            public boolean matches(URI uri) {
                return Objects.equals(uri.getHost(), host) &&
                        Objects.equals(uri.getRawPath(), format("/%s/%s", table, encodedKey));
            }

            public String toString() {
                return format("http://%s/%s/%s?...", host, table, encodedKey);
            }
        };
    }
}
