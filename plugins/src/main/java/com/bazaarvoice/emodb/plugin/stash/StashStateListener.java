package com.bazaarvoice.emodb.plugin.stash;

import com.bazaarvoice.emodb.plugin.Plugin;

import java.util.Date;

/**
 * Listener interface for Stash events.
 */
public interface StashStateListener<T> extends Plugin<T> {

    /**
     * Called whenever a scheduled Stash run is started.  Called once on each Stash instance.
     * Implementations can use this to verify that the expected number of Stash servers are active to participate
     * in the run.
     */
    void announceStashParticipation();

    /**
     * Called whenever a new Stash run starts.  Called once per Stash run and cluster.
     */
    void stashStarted(StashMetadata info);

    /**
     * Called whenever a Stash run completes.  Called once per Stash run and cluster.
     */
    void stashCompleted(StashMetadata info, Date completeTime);

    /**
     * Called whenever a Stash run is canceled.  Called once per Stash run and cluster.
     */
    void stashCanceled(StashMetadata info, Date canceledTime);
}
