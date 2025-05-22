FROM golang:1.23-alpine

RUN apk add --no-cache curl

WORKDIR /app
COPY . .

RUN go mod download
RUN go build -o controller ./cmd/controller
RUN go build -o node ./cmd/node

EXPOSE 8080 8081 8082 8083

CMD ["./controller"]