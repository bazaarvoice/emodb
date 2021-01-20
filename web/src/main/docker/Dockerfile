FROM amazoncorretto:8-alpine

ARG version
ARG APP_ROLE
ARG DATACENTER
ARG PORTS
ARG STARTUP_DELAY
ENV APP_ROLE=${APP_ROLE}
ENV DATACENTER=${DATACENTER}
ENV EMODB_VERSION $version

RUN addgroup -S emodb -g 1000
RUN adduser -S emodb -G emodb -u 1000

COPY --chown=1000 maven/emodb-web-* /app/emodb-web.jar
COPY --chown=1000 config/ /app/config/

WORKDIR /app

USER 1000

ENV JAVA_OPTS=""
EXPOSE ${PORTS}
ENTRYPOINT ["sh", "-c"]
CMD ["sleep ${STARTUP_DELAY} && java -jar emodb-web.jar server config-${APP_ROLE}-${DATACENTER}.yaml config-ddl-local-${DATACENTER}.yaml"]