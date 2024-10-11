package com.bazaarvoice.emodb.queue.core.kafka;

import software.amazon.msk.auth.iam.IAMClientCallbackHandler;

public class Reference {
    // This class is used solely to reference the aws-msk-iam-auth dependency
    public void reference() {
        IAMClientCallbackHandler handler = new IAMClientCallbackHandler();
    }
}

