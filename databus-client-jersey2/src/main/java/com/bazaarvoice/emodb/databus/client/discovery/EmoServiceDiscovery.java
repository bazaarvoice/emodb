package com.bazaarvoice.emodb.databus.client.discovery;


import java.net.URI;
import java.net.UnknownHostException;

//TODO add documentation and tests
public interface EmoServiceDiscovery {

    URI getBaseUri() throws UnknownHostException;
}
