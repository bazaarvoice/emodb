package com.bazaarvoice.emodb.plugin.migrator;


import com.bazaarvoice.emodb.plugin.PluginServerMetadata;
import com.bazaarvoice.emodb.plugin.stash.StashMetadata;
import com.bazaarvoice.emodb.plugin.stash.StashStateListener;
import io.dropwizard.setup.Environment;

import java.util.Date;

public class MigratorStateListener implements StashStateListener {
    @Override
    public void init(Environment environment, PluginServerMetadata metadata, Object config) {

    }

    @Override
    public void announceStashParticipation() {

    }

    @Override
    public void stashStarted(StashMetadata info) {

    }

    @Override
    public void stashCompleted(StashMetadata info, Date completeTime) {

    }

    @Override
    public void stashCanceled(StashMetadata info, Date canceledTime) {

    }
}
