.PHONY: build vet tidy clean test

GOLANG_VERSION := 1.23.4
PROXY_ENV := -e http_proxy=http://127.0.0.1:2081 -e https_proxy=http://127.0.0.1:2081 -e no_proxy=localhost,127.0.0.1
GO_ENV := -e GOPROXY='https://proxy.golang.org,direct' -e GONOSUMCHECK='*' -e GONOSUMDB='*'
DOCKER_RUN := docker run --rm --network=host $(PROXY_ENV) $(GO_ENV) -v $(HOME)/go/pkg/mod:/go/pkg/mod -v $(PWD):/app -w /app golang:$(GOLANG_VERSION) bash -c

build:
	$(DOCKER_RUN) 'go build -buildvcs=false -o mysql-2-elastic .'

vet:
	$(DOCKER_RUN) 'go vet ./...'

tidy:
	$(DOCKER_RUN) 'go mod tidy'

clean:
	rm -f mysql-2-elastic
