FROM amazoncorretto:11-alpine

ARG version
ARG APP_ROLE
ARG DATACENTER
ARG PORTS
ARG STARTUP_DELAY
ARG JAVA_OPTS
ARG LOCAL_HOST
ARG LOCAL_PORT
ARG LOCAL_ADMIN_PORT

ENV APP_ROLE=${APP_ROLE}
ENV DATACENTER=${DATACENTER}
ENV STARTUP_DELAY=${STARTUP_DELAY}
ENV EMODB_VERSION=${version}
ENV JAVA_OPTS="${JAVA_OPTS}"
ENV LOCAL_HOST=${LOCAL_HOST}
ENV LOCAL_PORT=${LOCAL_PORT}
ENV LOCAL_ADMIN_PORT=${LOCAL_ADMIN_PORT}

RUN addgroup -S emodb -g 1000
RUN adduser -S emodb -G emodb -u 1000

COPY --chown=1000 maven/emodb-web-* /app/emodb-web.jar

WORKDIR /app

USER 1000

EXPOSE ${PORTS}
ENTRYPOINT ["sh", "-c"]
CMD ["java -jar $JAVA_OPTS -Ddw.server.applicationConnectors[0].port=$LOCAL_PORT -Ddw.server.adminConnectors[0].port=$LOCAL_ADMIN_PORT emodb-web.jar server config/config-${APP_ROLE}-${DATACENTER}.yaml config/config-ddl-${DATACENTER}.yaml"]
