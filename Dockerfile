ARG TARGETARCH
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod ./
COPY . .
ENV CGO_ENABLED=0
RUN GOOS=linux \
    go build -o /app/app ./...

FROM --platform=$TARGETPLATFORM alpine:3.18
COPY --from=builder /app/app /usr/local/bin/app
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/app"]
