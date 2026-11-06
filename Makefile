.PHONY: deps
deps:
	go mod download

.PHONY: devel-deps
devel-deps:
	go install github.com/Songmu/godzil/cmd/godzil@v0.23.2

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

.PHONY: release
release: devel-deps
	godzil release
