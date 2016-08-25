package com.bazaarvoice.emodb.sor.core;

import com.google.inject.PrivateModule;


public class DataStoreAsyncModule extends PrivateModule{
    @Override
    protected void configure() {
        bind(DataStoreAsync.class).to(DefaultDataStoreAsync.class).asEagerSingleton();
        expose(DataStoreAsync.class);
    }
}