FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY . .
RUN apk add --no-cache build-base
RUN go build -o swarm-app ./examples/swarm/app

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/swarm-app .
EXPOSE 9090 8946
CMD ["./swarm-app"]
