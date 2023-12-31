FROM golang:1.18 as builder
RUN apt-get update && apt-get install -y librdkafka-dev
WORKDIR /app
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .
RUN GOOS=linux go build -o sample-producer

FROM ubuntu:20.04
RUN apt-get update && apt-get install -y ca-certificates
RUN mkdir /app
WORKDIR /app
COPY --from=builder /app/sample-producer .
CMD ./sample-producer export