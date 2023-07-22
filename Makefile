
.PHONY: test
test:
	@go test --race -cover --timeout 30s --coverprofile=coverage.out ./...

.PHONY: coverage
coverage: test
	@go tool cover -html coverage.out
