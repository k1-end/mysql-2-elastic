FROM golang:1.24-alpine3.21


WORKDIR /usr/src/app

COPY go.mod go.sum ./
RUN go mod download

RUN apk add --no-cache mysql-client && \
    apk add --no-cache jq && \
    apk add --no-cache mariadb-connector-c vim

COPY . .
RUN go build -v -o /usr/local/bin/app .

CMD ["app"]
