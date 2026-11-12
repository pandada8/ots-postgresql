GO ?= go
BIN := ots-postgresql
DSN ?= postgres://postgres:test@127.0.0.1:54321/ots?sslmode=disable

.PHONY: build test test-e2e run fmt vet docker clean

build:
	$(GO) build -o $(BIN) .

test:
	$(GO) test ./...

test-e2e:
	OTS_POSTGRES_TEST_DSN='$(DSN)' $(GO) test ./e2e/ -v

run: build
	./$(BIN) -postgres-dsn '$(DSN)'

fmt:
	gofmt -w .

vet:
	$(GO) vet ./...

docker:
	docker build -t ots-postgresql .

clean:
	rm -f $(BIN)
