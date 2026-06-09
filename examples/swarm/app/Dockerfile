FROM golang:1.25.6-alpine AS builder
WORKDIR /app
COPY . .
RUN apk add --no-cache build-base
RUN go build -o swarm-app ./examples/swarm/app

FROM alpine:latest
WORKDIR /root/
RUN apk add --no-cache curl
COPY --from=builder /app/swarm-app .
EXPOSE 8080 9090 8946
CMD ["./swarm-app"]
