package com.bazaarvoice.emodb.databus.client2.discovery;


import java.net.URI;
import java.net.UnknownHostException;

//TODO add documentation
public interface EmoServiceDiscovery {

    URI getBaseUri() throws UnknownHostException;
}
