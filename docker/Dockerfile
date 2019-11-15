FROM alpine:latest as build
WORKDIR /opt/emodb
RUN mkdir -p /opt/emodb
COPY ./web /opt/emodb/web
COPY ./docker /opt/emodb/config

FROM openjdk:8-jre-alpine
ARG APP_ROLE=web
ARG PORTS="8080 8081"
ARG VERSION
ARG STARTUP_DELAY=15
ENV APP_ROLE=${APP_ROLE}
ENV PORTS=${PORTS}
ENV VERSION=${VERSION}
ENV STARTUP_DELAY=${STARTUP_DELAY}
WORKDIR /opt/emodb
COPY --from=build /opt/emodb/web/target/emodb-web-${VERSION}.jar /opt/emodb
COPY --from=build /opt/emodb/config/config-${APP_ROLE}.yaml /opt/emodb
COPY --from=build /opt/emodb/config/config-ddl-local.yaml /opt/emodb
EXPOSE ${PORTS}
ENTRYPOINT ["sh", "-c"]
CMD ["sleep ${STARTUP_DELAY} && java -jar emodb-web-${VERSION}.jar server config-${APP_ROLE}.yaml config-ddl-local.yaml"]
