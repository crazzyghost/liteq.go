.DEFAULT_GOAL := check

.PHONY: lint test build fmt vet check release-dry clean

lint:
	golangci-lint run ./...

test:
	go test -race -coverprofile=coverage.out ./...

build:
	go build ./... 

fmt:
	gofumpt -w .

vet:
	go vet ./...

check: lint test build

release-dry:
	goreleaser release --snapshot --clean

clean:
	rm -f coverage.out
