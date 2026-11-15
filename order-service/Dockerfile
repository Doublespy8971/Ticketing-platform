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

# Copy sources for common dependencies and order-service
COPY ticketing-common ticketing-common
COPY ticketing-outbox ticketing-outbox
COPY ticketing-consumer ticketing-consumer
COPY order-service order-service

# Build package using Maven cache mount
RUN --mount=type=cache,target=/root/.m2 mvn -pl order-service -am clean package -DskipTests -B && \
    cp /workspace/order-service/target/order-service-*.jar /app.jar

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

RUN addgroup -S -g 10001 app && \
    adduser -S -u 10001 -G app -s /sbin/nologin app

COPY --from=build --chown=app:app /app.jar app.jar

USER app:app
EXPOSE 8082

ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"

HEALTHCHECK --interval=5s --timeout=3s --retries=10 --start-period=20s \
    CMD wget --no-verbose --tries=1 --spider http://127.0.0.1:8082/actuator/health || exit 1

ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-XX:+ExitOnOutOfMemoryError", "-jar", "app.jar"]
