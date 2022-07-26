# EmoDb Docker

Pretty much, this README is to tell you how to manually leverage Docker and Compose to build, initialize, manage and use EmoDb containers (including dependencies).

### What's included?

- EmoDb web
- EmoDb Megabus
- Cassandra (single-node)
- Kafka (single-node)
- Zookeeper (single-node)

### What's _not_ included?

- Anything about pushing images to a Docker repository. This is all just for local testing purposes at the moment. Long-term, we should add version control / appropriate tagging scheme / publishing ("docker push"-ing) stuff.

## Doing stuff

Docker is now included with mvn and docker image will be built if we select profile docker.

`mvn clean install -e -P docker`

Also, to speed things up, you can try skipping tests. I usually do ` -DskipTests -DskipITs` and then watch in awe as Maven proceeds to run all of the tests anyway.

### build Cassandra

It's based on the official image, but in order to supply our own `cassandra.yml` configuration, we have to "inherit" the official build. Again, running from `$GITROOT`:

```bash
  cd docker/
  docker build . -f ./cassandra-Dockerfile -t bazaarvoice/cassandra:3.11.12
```

Note that `docker-compose up` will build this for you if you skip this step and  the image hasn't been built before. If you make changes and want to rebuild, you can also skip this step and just include  the `--build` argument to `docker-compose up`, which will force rebuilding the Cassandra image.

### Services start

All services and apps (exclude kafka, zookeeper) available in 2 dc (dc1 and dc2).

Related services:

- cassandra
- zookeeper
- kafka

Available apps:

- emodb-web
- emodb-megabus
- emodb-stash

To start selected services use command:

```
docker-compose up -d service_name
```

example: will launch 1 C* node, kafka, zookeeper, 2 web and 2 megabus. Check with `docker ps`

```
➜ docker-compose up -d emodb-megabus-dc2
Creating network "docker_emodb" with driver "bridge"
Creating volume "docker_zookeeper_data" with local driver
Creating volume "docker_cassandra-dc1_data" with local driver
Creating volume "docker_kafka_data" with local driver
Creating docker_cassandra-dc1_1 ... done
Creating docker_zookeeper_1     ... done
Creating docker_kafka_1         ... done
Creating docker_emodb-web-dc1_1 ... done
Creating docker_emodb-web-dc2_1 ... done
Creating docker_emodb-megabus-dc2_1 ... done
```

Stopping and deleting services and apps:

```
➜ docker-compose down --remove-orphans -v
Stopping docker_emodb-web-dc1_1 ... done
Stopping docker_zookeeper_1     ... done
Stopping docker_cassandra-dc1_1 ... done
Removing docker_emodb-web-dc2_1 ... done
Removing docker_emodb-web-dc1_1 ... done
Removing docker_zookeeper_1     ... done
Removing docker_cassandra-dc1_1 ... done
Removing network docker_emodb
Removing volume docker_zookeeper_data
Removing volume docker_cassandra-dc1_data
Removing volume docker_kafka_data
```

To override host for local integration tests:
```bash
export EMODB_WEB_DC1_LOCAL_HOST=127.0.0.1; docker-compose up -d emodb-web-dc1
...
#run tests
...
docker-compose down --remove-orphans -v 
```

### Logs
In containers the app configured with 2 appenders:
- console
- file

The file is used to store logs inside a container in the folder `/app/logs/`. This folder mounted outside of container in `web-local/target/logs/`.

## References / useful but disorganized info that may save you from eating your hat/shoe/umbrella

### Gotchas / surprising behavior

- Sometimes you need to cleanup stuff and you don't know how, or Docker won't let you because somehow it's "in-use." The internet will tell you to restart Docker (like, "Docker Desktop for OS X" or whatever -- the program that runs with the little task bar icon). That didn't work for me. I had to "purge" containers, and _then_ "purge" volumes with `docker container purge` and then `docker volume purge`. There is a `docker system purge`  or something, but that apparently also clears  build caches etc., which can be expensive to rebuild.
-  There's  a `--entrypoint` option for `docker  run` that's handy for debugging. It  overrides the `ENTRYPOINT`  of the image.  Don't ask  me what `ENTRYPOINT` _actually_ does, but you can say something like `docker run  --entrypoint bash  -it <image>` to login to a container when  you run it.
- You can run individual services to test them in isolation, like with `docker run -it bazaarvoice/emodb-megabus:latest`
- You can attach a debugger like IntelliJ IDEA to a running process with a couple easy (albeit tedious) steps. First, add `-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005` to the `java -jar` line (before the `-jar` option) of the Dockerfile (currently in `$GITROOT/docker/Dockerfile`). Then, build the image. Next, update `docker-compose.yml` and expose port `5005` (there's a commented one in there at the moment, so it should be hard to miss the example). Finally, `docker-compose up` and wait until you see that familiar line about waiting for a debugger to attach, and then attach your debugger per usual. Yes, this really works.

### Documentation references

- [Kafka docker image reference](https://docs.confluent.io/current/installation/docker/index.html) (helpful for things like knowing how to configure Kafka)
- [Kafka docker image on DockerHub](https://hub.docker.com/r/confluentinc/cp-kafka), because the image just named "kafka" is a packaged product. I don't know how they get away with this legally but whatever, I guess strictly speaking they didn't modify the software at all.
- [Someone wrote this about Kafka and Compose](https://rmoff.net/2018/08/02/kafka-listeners-explained/) presumably to clear up the otherwise incredibly confusing networking concepts
- [Cassandra on DockerHub](https://hub.docker.com/_/cassandra), this is what I based the [Cassandra Dockerfile](./cassandra-Dockerfile) on. We have our own Dockerfile that does some kind of weird inheritance and allows us to override the Cassandra configuration that is used in the container. Don't ask me how this works (there's lengthy documentation on the subject), but the documentation written for the Cassandra Docker image on DockerHub helpfully explains that the entire process of setting your own Cassandra configuration is left as an "exercise for the reader." Not that I felt like I needed it, but I certainly feel exercised.
