Operational Menu
================

The server exposes an administration / operations interface on a different port, [8081 by default] (http://localhost:8081/).
Through this API you can:

1.  Check that the server is responding to requests  ([local url] (http://localhost:8081/ping)):

    Example:

        $ curl -s "http://localhost:8081/ping"
        pong

2.  Check that the server's dependencies appear to be healthy (eg. ping Cassandra) ([local url] (http://localhost:8081/healthcheck)):

    Example:

        $ curl -s "http://localhost:8081/healthcheck"
        * deadlocks: OK
        * emo-cassandra: OK
          127.0.0.1(127.0.0.1):9160 879us

3.  Get a stack dump of all Java threads ([local url] (http://localhost:8081/threads)):

    Example:

        $ curl -s "http://localhost:8081/threads"
        main id=1 state=WAITING
            - waiting on <0x008d41f2> (a java.lang.Object)
            - locked <0x008d41f2> (a java.lang.Object)
            at java.lang.Object.wait(Native Method)
            at java.lang.Object.wait(Object.java:485)
            at org.eclipse.jetty.util.thread.QueuedThreadPool.join(QueuedThreadPool.java:386)
            at org.eclipse.jetty.server.Server.join(Server.java:398)
        ...

4.  Get a JSON description of the performance of many aspects of the server, including per-API call counts and average
    response times ([local url] (http://localhost:8081/metrics?pretty=true))

    Example:

        $ curl -s "http://localhost:8081/metrics" | jsonpp
        {
          "jvm" : {
            "memory" : {
              "totalInit" : 8.501344E7,
              "totalUsed" : 3.3927232E7,
              "totalMax" : 1.0551296E9,
              "totalCommitted" : 1.00401152E8,
              "heapInit" : 6.5876928E7,
              "heapUsed" : 1.57754E7,
              "heapMax" : 9.37689088E8,
              "heapCommitted" : 8.0478208E7,
              "heap_usage" : 0.016823700096209288,
              "non_heap_usage" : 0.15456192834036692,
              "memory_pool_usages" : {
                "Code Cache" : 0.013985951741536459,
                "PS Eden Space" : 0.03663812032560023,
                "PS Old Gen" : 2.7373963283943716E-4,
                "PS Perm Gen" : 0.25999391078948975,
                "PS Survivor Space" : 0.8124847412109375
              }
            },
        ...
