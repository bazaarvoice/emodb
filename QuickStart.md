### Starting EmoDB locally using only EmoDB binaries 

To start EmoDB locally, simply download the [EmoDB binaries] (https://github.com/bazaarvoice/emodb/releases), and run the following:

```
$> ./start-local.sh
``` 

The above will start the following services:

-  In-memory Cassandra
-  Zookeeper on port 2181
-  EmoDB on port 8080/8081

If you would like to use your own configured cassandra, then start the local jar directly:
```
java -jar bin/emodb-web-local-*.jar server conf/config-local.yaml conf/config-ddl-local.yaml conf/cassandra.yaml -z
```
To skip starting a local zookeeper, leave out `-z` switch. 



