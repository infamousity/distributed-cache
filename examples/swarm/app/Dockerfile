FROM golang:1.25.6-alpine@sha256:98e6cffc31ccc44c7c15d83df1d69891efee8115a5bb7ede2bf30a38af3e3c92 AS builder
WORKDIR /app
COPY . .
RUN apk add --no-cache build-base
RUN go build -o swarm-app ./examples/swarm/app

FROM alpine:3.23.2@sha256:865b95f46d98cf867a156fe4a135ad3fe50d2056aa3f25ed31662dff6da4eb62
WORKDIR /app
RUN apk add --no-cache curl
COPY --from=builder --chown=65532:65532 /app/swarm-app .
USER 65532:65532
EXPOSE 8080 9090 8946
CMD ["./swarm-app"]
