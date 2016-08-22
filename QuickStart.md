### Starting EmoDB locally using only EmoDB binaries 

To start EmoDB locally, simply download the binaries, and run the following:
 
```
java -jar bin/emodb-web-local-*.jar server conf/config-local.yaml conf/config-ddl-local.yaml conf/cassandra.yaml -z
```
The above will start the following services:

-  In-memory Cassandra
-  Zookeeper on port 2181
-  EmoDB on port 8080/8081

If you would like to use your own configured cassandra, then skip providing cassandra.yaml and update Emo's config file to point to the correct Cassandra node.

To skip starting zookeeper, leave out `-z` switch. 



