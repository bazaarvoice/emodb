FROM alpine:latest as build
WORKDIR /opt/emodb
RUN mkdir -p /opt/emodb
COPY ./web /opt/emodb/web
COPY ./docker /opt/emodb/config

FROM openjdk:8-jre-alpine
ARG APP_ROLE
ARG DATACENTER
ARG PORTS
ARG STARTUP_DELAY
ENV APP_ROLE=${APP_ROLE}
ENV DATACENTER=${DATACENTER}
ENV STARTUP_DELAY=${STARTUP_DELAY}
WORKDIR /opt/emodb
COPY  --from=build /opt/emodb/web/target/emodb-web*.jar /opt/emodb
COPY  --from=build /opt/emodb/config/config-${APP_ROLE}-${DATACENTER}.yaml /opt/emodb
COPY  --from=build /opt/emodb/config/config-ddl-local-${DATACENTER}.yaml /opt/emodb
EXPOSE ${PORTS}
ENTRYPOINT ["sh", "-c"]
CMD ["sleep ${STARTUP_DELAY} && java -jar emodb-web-*.jar server config-${APP_ROLE}-${DATACENTER}.yaml config-ddl-local-${DATACENTER}.yaml"]
