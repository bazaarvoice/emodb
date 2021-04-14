Quality
============

Quality Maven Module contains various unit and integration http endpoint tests:
- [jersey unit tests](./integration/src/test/java/test/integration)
- [blackbox tests](./integration/src/test/java/test/blackbox) run against actual EmoDB / C* processes started by mvn using emodb-sdk.
- [emodb client tests](./integration/src/test/java/test/client) run against actual EmoDB / C* processes started by mvn using emodb-sdk.
- [postman endpoint tests](postman)

### How to run:
#### To run all integration tests locally:
```bash
mvn clean verify -Ddependency-check.skip=true
```
#### To run client integration tests
To spin up emodb-web infrastructure locally and run client integration tests 
```bash
mvn clean verify -Dintegration.suiteXmlFile=src/test/resources/client-testng.xml -Ddependency-check.skip=true
```

To run client integration tests against existing emodb-web infrastructure
```bash
mvn clean verify -P!'start-emodb' -Dintegration.suiteXmlFile=src/test/resources/client-testng.xml -DapiKey=<apiKey> -DclusterName=<clusterName> -DzkConnection=<zkConnection> -DzkNamespace=<zkNamespace> -Dplacement=<placement> -DremotePlacement=<remotePlacement> -DmediaPlacement=<mediaPlacement> Ddependency-check.skip=true
```
