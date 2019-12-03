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

Note: all commands run from $GITROOT. In my case, my git root is `/work/emodb`, so just export to sub in the value: `export GITROOT=/work/emodb` and `cd $GITROOT`.

### create artifacts

    mvn clean install

Note that the following two steps for building emodb-web and emodb-megabus should become obsolete when the maven build integrates with Docker, _but_ we should keep them in the documentation anyway ~~in case~~ so that when someone needs to know how to build an image without waiting a brief lifetime for the entire repo to build, they can.

Also, to speed things up, you can try skipping tests. I usually do ` -DskipTests -DskipITs` and then watch in awe as Maven proceeds to run all of the tests anyway.

### build emodb-web

Be careful not to use `/` for `$GITROOT` because apparently there's no protection against potentially copying literally your entire filesystem to the Docker daemon.

    docker build $GITROOT -f ./docker/Dockerfile -t bazaarvoice/emodb-web:latest --build-arg VERSION=6.0.12-SNAPSHOT --build-arg APP_ROLE=web --build-arg PORTS="8080 8081" --build-arg STARTUP_DELAY=15

### build emodb-megabus

    docker build $GITROOT -f ./docker/Dockerfile -t bazaarvoice/emodb-megabus:latest --build-arg VERSION=6.0.12-SNAPSHOT --build-arg APP_ROLE=megabus --build-arg PORTS="8082 8083" --build-arg STARTUP_DELAY=30

Why `STARTUP_DELAY`? And why is it longer for Megabus than Web? Because Megabus depends on Web, and I'm baking a `sleep` into the _actual build_ because although Docker Compose allows you to define the order of starting up services, it does _not wait_ for those services to complete their startup protocols before starting the services that depend on them. Docker [documentation acknowledges this](https://docs.docker.com/compose/startup-order/) and has a rather lengthy explanation that essentially amounts to them punting on the problem and blaming the user, saying (paraphrased) "Your services should be resilient to unavailable connections and build retries into the application." [Some people](https://8thlight.com/blog/dariusz-pasciak/2016/10/17/docker-compose-wait-for-dependencies.html) have tried to solve this other ways, but when I tried to do it this way, it still didn't work (Compose would startup everything without waiting anyway).

### build Cassandra 2.2.4

It's based on the official image, but in order to supply our own `cassandra.yml` configuration, we have to "inherit" the official build. Again, running from `$GITROOT`:

    docker build $GITROOT -f ./docker/cassandra-Dockerfile -t bazaarvoice/cassandra:2.2.4

Note that `docker-compose up` will build this for you if you skip this step and  the image hasn't been built before. If you make changes and want to rebuild, you can also skip this step and just include  the `--build` argument to `docker-compose up`, which will force rebuilding the Cassandra image.

### start all services

    docker-compose -f ./docker/docker-compose.yml up

### stop all services

    docker-compose -f ./docker/docker-compose.yml down

This is basically the same as Ctrl+C after running `docker-compose [...] up`, _but_ this does not cleanup/delete any data or temporary files. In fact, the Docker volumes, network and other resources ~~will~~ may hang around so if you are having trouble starting up services because of suspected lingering  data, you need to invoke the above command with the  `--volumes` option (just add it  to the end of the `down` command). This will, apparently, cleanup volumes and give you fresh, empty ones.

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
- [Cassandra 2.2.4 on DockerHub](https://hub.docker.com/_/cassandra), this is what I based the [Cassandra Dockerfile](./cassandra-Dockerfile) on. We have our own Dockerfile that does some kind of weird inheritance and allows us to override the Cassandra configuration that is used in the container. Don't ask me how this works (there's lengthy documentation on the subject), but the documentation written for the Cassandra Docker image on DockerHub helpfully explains that the entire process of setting your own Cassandra configuration is left as an "exercise for the reader." Not that I felt like I needed it, but I certainly feel exercised.
