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

RUN addgroup -S emodb -g 1000
RUN adduser -S emodb -G emodb -u 1000

COPY --chown=1000 maven/emodb-web-* /app/emodb-web.jar

WORKDIR /app

USER 1000

ENV JAVA_OPTS=""
EXPOSE ${PORTS}
ENTRYPOINT ["sh", "-c"]
CMD ["java -jar emodb-web.jar server config/config-${APP_ROLE}-${DATACENTER}.yaml config/config-ddl-${DATACENTER}.yaml"]
