# Stage 1: Build static Go binary
FROM golang:1.23.2-alpine AS build
WORKDIR /src
ENV GOTOOLCHAIN=auto

# Cache dependencies
COPY notification-service/go.mod notification-service/go.sum ./
RUN go mod download

# Copy source and build static binary (CGO_ENABLED=0)
COPY notification-service/cmd/ cmd/
COPY notification-service/internal/ internal/
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /notification-service ./cmd/notification-service

# Stage 2: Distroless static runtime (no shell, no package manager)
# Pinned by multi-arch index digest resolved via `docker buildx imagetools inspect gcr.io/distroless/static-debian12:nonroot`
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab

COPY --from=build --chown=nonroot:nonroot /notification-service /notification-service

USER nonroot:nonroot
EXPOSE 8084

# Health check using exec form directly invoking -healthcheck flag (no shell required)
HEALTHCHECK --interval=5s --timeout=3s --retries=3 --start-period=5s \
    CMD ["/notification-service", "-healthcheck"]

ENTRYPOINT ["/notification-service"]
