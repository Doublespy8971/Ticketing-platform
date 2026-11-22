FROM maven:3.9.9-eclipse-temurin-21-alpine AS build
WORKDIR /workspace

# Copy root and module POMs to allow reactor resolution
COPY pom.xml .
COPY ticketing-common/pom.xml ticketing-common/
COPY ticketing-outbox/pom.xml ticketing-outbox/
COPY ticketing-consumer/pom.xml ticketing-consumer/
COPY gateway-service/pom.xml gateway-service/
COPY inventory-service/pom.xml inventory-service/
COPY order-service/pom.xml order-service/
COPY payment-service/pom.xml payment-service/

# Copy sources for common dependencies and gateway-service
COPY ticketing-common ticketing-common
COPY gateway-service gateway-service

# Build package using Maven cache mount
RUN --mount=type=cache,target=/root/.m2 mvn -pl gateway-service -am clean package -DskipTests -B && \
    cp /workspace/gateway-service/target/gateway-service-*.jar /app.jar

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

RUN addgroup -S -g 10001 app && \
    adduser -S -u 10001 -G app -s /sbin/nologin app

COPY --from=build --chown=app:app /app.jar app.jar

USER app:app
EXPOSE 8080

ENV JAVA_OPTS="-XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError -Xlog:gc+init=info"
ENV JAVA_OPTS_EXTRA=""

HEALTHCHECK --interval=5s --timeout=3s --retries=10 --start-period=20s \
    CMD wget --no-verbose --tries=1 --spider http://127.0.0.1:8080/actuator/health || exit 1

ENTRYPOINT ["sh", "-c", "exec java ${JAVA_OPTS_EXTRA:-$JAVA_OPTS} -jar app.jar"]
