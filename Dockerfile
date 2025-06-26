FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY . .
RUN apk add --no-cache build-base
RUN go build -o cache-node ./main.go

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/cache-node .
COPY --from=builder /app/config.yml .
COPY --from=builder /app/.env .
EXPOSE 8080 8946
CMD ["./cache-node"]
