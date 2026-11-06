.PHONY: deps
deps:
	go mod download

.PHONY: test
test:
	go test -race ./...

.PHONY: lint
lint:
	go vet ./...
	@files="$$(gofmt -l .)"; if [ -n "$$files" ]; then printf 'Run gofmt on:\n%s\n' "$$files"; exit 1; fi

.PHONY: cover
cover:
	go test -race -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -func=coverage.out
