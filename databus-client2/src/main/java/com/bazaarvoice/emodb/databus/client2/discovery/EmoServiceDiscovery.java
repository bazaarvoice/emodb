package com.bazaarvoice.emodb.databus.client2.discovery;


import java.net.URI;
import java.net.UnknownHostException;

public interface EmoServiceDiscovery {

    public URI getBaseUri() throws UnknownHostException;
}
