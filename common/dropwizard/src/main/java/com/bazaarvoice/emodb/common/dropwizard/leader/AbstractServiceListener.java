package com.bazaarvoice.emodb.common.dropwizard.leader;

import com.google.common.util.concurrent.Service;

class AbstractServiceListener extends Service.Listener {
    @Override
    public void starting() {
    }

    @Override
    public void running() {
    }

    @Override
    public void stopping(Service.State from) {
    }

    @Override
    public void terminated(Service.State from) {
    }

    @Override
    public void failed(Service.State from, Throwable failure) {
    }
}
