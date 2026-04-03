.DEFAULT_GOAL := check

.PHONY: lint test build fmt vet check install release-dry clean

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

install:
	go install ./cmd/lq

release-dry:
	goreleaser release --snapshot --clean

clean:
	rm -f coverage.out
