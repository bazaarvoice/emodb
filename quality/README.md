Quality
============
TODO

### Running tests within IntelliJ

- Add VM Options to configuration
  - Run
  - Edit Configuration
  - Expand 'Default'
  - Click on TestNG
  -  Add following line to default TestNG ```VM Options```
    ```
    -ea -DapiKey=<apiKey>                             \ 
        -DzkConnection=localhost:2181 -DzkNamespace=  \
        -DclusterName=local_default                   \
        -Dplacement=ugc_global:ugc -DremotePlacement=app_remote:default -DmediaPlacement=blob_global:media \
        -DclientHttpTimeout=10 -DclientHttpKeepAlive=1
    ```
- Run tests
  - Right click on method/class
  - Click 'Run <Name Here>'
  

### Running on Command line
```bash
mvn clean verify -P                                 \
      -DapiKey=<apiKey>                             \
      -DclusterName=local_default                   \
      -DzkConnection=localhost:2181 -DzkNamespace=  \
      -Dplacement=ugc_global:ugc -DremotePlacement=app_remote:default -DmediaPlacement=blob_global:media \
      -DclientHttpTimeout=10 -DclientHttpKeepAlive=1
```
