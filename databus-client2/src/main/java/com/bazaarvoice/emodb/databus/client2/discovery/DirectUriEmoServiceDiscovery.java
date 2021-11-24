package com.bazaarvoice.emodb.databus.client2.discovery;

import java.net.URI;
import java.net.UnknownHostException;

public class DirectUriEmoServiceDiscovery implements EmoServiceDiscovery {

    private final URI _baseUri;

    public DirectUriEmoServiceDiscovery(URI baseUri) {
        _baseUri = baseUri;
    }

    @Override
    public URI getBaseUri() throws UnknownHostException {
        return _baseUri;
    }

}
