FROM golang:1.25.6-alpine AS builder

WORKDIR /app
COPY . .
RUN apk add --no-cache build-base
RUN go build -o cache-node ./main.go

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/cache-node .
COPY --from=builder /app/config.yml .
COPY --from=builder /app/config.swarm.example.yml .
COPY --from=builder /app/.env .
EXPOSE 9090 8946
CMD ["./cache-node"]
