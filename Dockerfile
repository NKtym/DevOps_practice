FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod ./
COPY . .
ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOARCH=amd64 
RUN go build -o /app/app ./...

FROM alpine:3.18
COPY --from=builder /app/app /usr/local/bin/app
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/app"]
