FROM golang:1.23.2-alpine AS build
WORKDIR /src

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build static binary
COPY cmd/ cmd/
COPY internal/ internal/
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /notification-service ./cmd/notification-service

FROM alpine:3.20

# Create non-root user and group
RUN addgroup -S -g 10001 app && \
    adduser -S -u 10001 -G app -s /sbin/nologin app

COPY --from=build --chown=app:app /notification-service /notification-service

USER app:app
EXPOSE 8084

HEALTHCHECK --interval=5s --timeout=3s --retries=3 --start-period=5s \
    CMD wget --spider -q http://localhost:8084/health || exit 1

ENTRYPOINT ["/notification-service"]
