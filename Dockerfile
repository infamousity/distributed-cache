FROM golang:1.25.6-alpine@sha256:98e6cffc31ccc44c7c15d83df1d69891efee8115a5bb7ede2bf30a38af3e3c92 AS builder

WORKDIR /app
COPY . .
RUN apk add --no-cache build-base
RUN go build -o cache-node ./main.go

FROM alpine:3.23.2@sha256:865b95f46d98cf867a156fe4a135ad3fe50d2056aa3f25ed31662dff6da4eb62
WORKDIR /app
COPY --from=builder --chown=65532:65532 /app/cache-node .
COPY --from=builder --chown=65532:65532 /app/config.yml .
COPY --from=builder --chown=65532:65532 /app/config.swarm.example.yml .
USER 65532:65532
EXPOSE 9090 8946
CMD ["./cache-node"]
