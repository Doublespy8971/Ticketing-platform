FROM golang:1.23.2-alpine AS build
WORKDIR /src
COPY go.mod .
COPY main.go .
RUN CGO_ENABLED=0 GOOS=linux go build -o /notification-service main.go

FROM alpine:3.20
COPY --from=build /notification-service /notification-service
EXPOSE 8084
ENTRYPOINT ["/notification-service"]
