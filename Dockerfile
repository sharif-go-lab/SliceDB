FROM golang:1.23-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /out/controller ./cmd/controller \
 && CGO_ENABLED=0 go build -o /out/node ./cmd/node \
 && CGO_ENABLED=0 go build -o /out/loadbalancer ./cmd/loadbalancer \
 && CGO_ENABLED=0 go build -o /out/client ./cmd/client

FROM alpine:3.20

RUN apk add --no-cache curl

WORKDIR /app
COPY --from=build /out/ /app/

EXPOSE 7000 8080 9000

CMD ["./controller"]
