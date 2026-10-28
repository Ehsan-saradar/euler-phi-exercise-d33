BIN := bin
GO ?= go

.PHONY: all build test test-unit test-integration lint proto proto-check run up down logs demo chaos bench clean

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
