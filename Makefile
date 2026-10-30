BIN := bin
GO ?= go

.PHONY: all build test test-unit test-integration test-docker mac-aliases lint proto proto-check run up down logs demo chaos bench clean

all: lint test build

build:
	$(GO) build -o $(BIN)/kvnode ./cmd/kvnode
	$(GO) build -o $(BIN)/kvctl ./cmd/kvctl

test:
	$(GO) test -race -count=1 ./...

test-unit:
	$(GO) test -race -count=1 ./internal/...

test-integration:
	$(GO) test -race -count=1 -v ./test/integration/...

# Full suite in a Linux container: no local Go toolchain or loopback aliases needed.
test-docker:
	docker run --rm -v "$(CURDIR)":/src -w /src -v kv-gomod:/go/pkg/mod -v kv-gocache:/root/.cache/go-build \
		golang:1.26 go test -race -count=1 ./...

# The in-process harness binds 127.0.1.1-5. Linux has them already; macOS needs
# aliases on lo0 (sudo, lost on reboot).
mac-aliases:
	for i in 1 2 3 4 5; do sudo ifconfig lo0 alias 127.0.1.$$i up; done

lint:
	golangci-lint run ./...
	buf lint

proto:
	buf generate

proto-check: proto
	git diff --exit-code -- gen/ || (echo "gen/ is stale: run make proto" && exit 1)

up:
	docker compose up --build -d

down:
	docker compose down -v

logs:
	docker compose logs -f

demo:
	./scripts/demo.sh

chaos:
	$(GO) test -count=1 -v -run TestLinearizability -timeout 30m ./test/linearizability/...

bench:
	./scripts/bench.sh

clean:
	rm -rf $(BIN)
