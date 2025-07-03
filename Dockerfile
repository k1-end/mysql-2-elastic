FROM base-go


WORKDIR /usr/src/app

COPY go.mod go.sum ./
RUN go mod download

RUN apk add --no-cache mysql-client && \
    apk add --no-cache mariadb-connector-c

COPY . .
RUN go build -v -o /usr/local/bin/app ./...

CMD ["app"]
