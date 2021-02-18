FROM amazoncorretto:8-alpine

ARG version
ARG APP_ROLE
ARG DATACENTER
ARG PORTS
ARG STARTUP_DELAY
ENV APP_ROLE=${APP_ROLE}
ENV DATACENTER=${DATACENTER}
ENV STARTUP_DELAY=${STARTUP_DELAY}
ENV EMODB_VERSION=${version}
ENV CASSANDRA_VERSION=2.1.22

RUN addgroup -S emodb -g 1000
RUN adduser -S emodb -G emodb -u 1000

COPY --chown=1000 maven/emodb-web-* /app/emodb-web.jar

RUN apk update \
 && apk add busybox-extras

WORKDIR /app

USER 1000

RUN wget -q http://archive.apache.org/dist/cassandra/${CASSANDRA_VERSION}/apache-cassandra-${CASSANDRA_VERSION}-bin.tar.gz \
 && tar zxf apache-cassandra-${CASSANDRA_VERSION}-bin.tar.gz \
 && mv -v apache-cassandra-${CASSANDRA_VERSION} cassandra \
 && rm -fv apache-cassandra-${CASSANDRA_VERSION}-bin.tar.gz

ENV PATH="/app/cassandra/bin:${PATH}"
ENV JAVA_OPTS=""
EXPOSE ${PORTS}
ENTRYPOINT ["sh", "-c"]
CMD ["java -jar emodb-web.jar server config/config-${APP_ROLE}-${DATACENTER}.yaml config/config-ddl-${DATACENTER}.yaml"]
