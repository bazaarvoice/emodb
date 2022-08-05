EmoDB Plugins
=============

While EmoDB provides full functionality out-of-the-box there are some cases where it may be useful to integrate
custom functionality into the EmoDB without forking the source code or requiring a custom build.  To that end
EmoDB provides a plugin architecture.  All plugins inherit from
[Plugin<T>] (https://github.com/bazaarvoice/emodb/blob/main/plugins/src/main/java/com/bazaarvoice/emodb/plugin/Plugin.java).

Plugin types
------------

There are currently two plugins supported by EmoDB:

### [ServerStartedListener] (https://github.com/bazaarvoice/emodb/blob/main/plugins/src/main/java/com/bazaarvoice/emodb/plugin/lifecycle/ServerStartedListener.java)

It is frequently useful for the server to perform custom actions once it is started and available for requests.
A common example of this is registering the server with a service registry.  The `ServerStartedListener` plugin provides
a single method, `serverStarted()`, which is called immediately after the server is fully started.

The following example demonstrates configuring the provided implementation
[LoggingServerStartedListener] (https://github.com/bazaarvoice/emodb/blob/main/plugins/src/main/java/com/bazaarvoice/emodb/plugin/lifecycle/LoggingServerStartedListener.java)
in the server's `config.yaml`:

```
serverStartedListeners:
   - class: com.bazaarvoice.emodb.plugin.lifecycle.LoggingServerStartedListener
```

### [StashStateListener] (https://github.com/bazaarvoice/emodb/blob/main/plugins/src/main/java/com/bazaarvoice/emodb/plugin/stash/StashStateListener.java)

When Stash is running it is useful to have callbacks on certain Stash events for monitoring and alerting.
`StashStateListener` provides methods that are called when these events occur.  Implementations can use these events
to perform custom actions, such as notifying an external monitoring resource when they occur.  The events supported
by this method are:

* `announceStashParticipation`
  * Called whenever a scheduled Stash run is started.  Called once on each Stash instance.  Implementations can use this to
    verify that the expected number of Stash servers are active to participate in the run.
* `stashStarted`
  * Called whenever a new Stash run starts.  Called once per Stash run and cluster.
* `stashCompleted`
  * Called whenever a Stash run completes.  Called once per Stash run and cluster.
* `stashCanceled`
  * Called whenever a Stash run is canceled.  Called once per Stash run and cluster.

The following example demonstrates configuring the provided implementation
[LoggingStashStateListener] (https://github.com/bazaarvoice/emodb/blob/main/plugins/src/main/java/com/bazaarvoice/emodb/plugin/stash/LoggingStashStateListener.java)
in the server's `config.yaml`:

```
scanner:
  notifications:
    stashStateListeners:
      - class: com.bazaarvoice.emodb.plugin.stash.LoggingStashStateListener
```

Creating a plugin
-----------------

Each plugin class must provide all of the following:

* A public zero-argument constructor
* An implementation of `init(Environment, PluginServerMetadata, T)` (more on this below).
* Implementations of the plugin-specific interface methods.

Configuring a plugin
--------------------

A plugin can accept a single configuration argument which is passed to its `init` method.  The plugin class provides
a configuration class as its plugin's generic parameter.  The server's `config.yaml` includes the fully-qualified
implementation's class name and a configuration section that can be JSON-parsed into the configuration class.

If the plugin does not require custom configuration then specify `Void` as the configuration class.

Lifecycle of a plugin
---------------------

Each plugin is instantiated and initialized at some point during the server's startup, specifically during the DropWizard
application's `run()` call.  As such some aspects of the server may be available at the time `init` is called but
the service is not fully initialized and available at the time.

The `init` method accepts three parameters:

1. `Environment` is the DropWizard environment instance passed to `run()`.  The plugin can use this just as any DropWizard
   service can.  However, implementations are encouraged to restrict usage to operations that will not interfere with the
   rest of the EmoDB system.  For example, adding custom metrics or registering new `Managed` objects is fine, but adding a
   custom Jersey resource filter may potentially break other parts of EmoDB.
2. `PluginServerMetadata` is an object that provides key metadata about the system, such as the service mode, cluster name,
   service and administrator ports, and the singleton ZooKeeper connection used by EmoDB.
3. The third parameter is the instantiation of the plugin's configuration class.  If the configuration class was `Void`
   then this parameter will be null.

Example
-------

The following example demonstrates creating a simple `ServerStartedListener` that logs a customizable message
when the server starts:

Source code for `com.samplecorp.emodb.GreetingServerStartedListener`:

```java
package com.samplecorp.emodb;

import com.bazaarvoice.emodb.plugin.PluginServerMetadata;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GreetingServerStartedListener implements ServerStartedListener<GreetingServerStartedListener.Configuration> {

    private String cluster;
    private String message;

    @Override
    public void init(Environment environment, PluginServerMetadata metadata, Configuration config) {
        cluster = metadata.getCluster();
        message = config.message;
    }

    @Override
    public void serverStarted() {
        Logger log = LoggerFactory.getLogger(getClass());
        log.info("Cluster {} says \"{}\"", cluster, message);
    }

    public static class Configuration {
        @JsonProperty
        private String message;
    }
}
```

Relevant portion of `config.yaml`:

```
serverStartedListeners:
   - class: com.samplecorp.emodb.GreetingServerStartedListener
     config:
       message: Greetings from EmoDB!

```
