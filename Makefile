.PHONY: run build
build:
	GOOS=linux GOARCH=amd64 go build -tags consumer -o bin/go-consumer cmd/main.go
	docker build -f build/docker/Dockerfile -t consumer  .
run:
	docker run --rm  -e MICRO_REGISTRY=mdns consumer
