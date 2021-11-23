package com.bazaarvoice.emodb.databus.client2.discovery;

import java.net.URI;
import java.net.UnknownHostException;

public class DirectUriEmoServiceDiscovery implements EmoServiceDiscovery {
    private URI _baseUri;

    public DirectUriEmoServiceDiscovery(URI directUri) {
        _baseUri = directUri;
    }

    @Override
    public URI getBaseUri() throws UnknownHostException {
        return _baseUri;
    }

}
