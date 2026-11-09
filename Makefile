# Uses a local Go toolchain if installed, otherwise runs Go inside Docker.
ifeq ($(shell command -v go 2>/dev/null),)
GO_IMAGE := golang:1.27
DOCKER_GO = docker run --rm -u $$(id -u):$$(id -g) -v $(CURDIR):/src -w /src \
	-v $(HOME)/.cache/go-docker:/gocache -e GOMODCACHE=/gocache/mod -e GOCACHE=/gocache/build \
	-e HOME=/tmp $(1) $(GO_IMAGE)
GO := $(call DOCKER_GO,) go
GO_ON_NET = $(call DOCKER_GO,--network linkpulse_default $(1)) go
INT_DB := postgres://linkpulse:linkpulse@postgres:5432/linkpulse?sslmode=disable
INT_REDIS := redis://redis:6379/1
else
GO := go
GO_ON_NET = $(1) go
INT_DB := postgres://linkpulse:linkpulse@localhost:5433/linkpulse?sslmode=disable
INT_REDIS := redis://localhost:6380/1
endif

.PHONY: up down logs test test-integration lint fmt build loadtest clean

up: ## build and start the whole stack
	docker compose up -d --build
	@echo "API: http://localhost:8080"

down:
	docker compose down

logs:
	docker compose logs -f api analytics

test: ## unit tests with the race detector
	@mkdir -p $(HOME)/.cache/go-docker
	$(GO) test -race ./...

test-integration: ## tests against real Postgres + Redis (needs `make up`)
	@mkdir -p $(HOME)/.cache/go-docker
	$(call GO_ON_NET,-e DATABASE_URL=$(INT_DB) -e REDIS_URL=$(INT_REDIS)) test -race -tags integration -count=1 ./internal/

lint:
	$(GO) vet ./...
	@test -z "$$($(GO)fmt -l . 2>/dev/null)" || (echo "run make fmt"; exit 1)

fmt:
	$(GO) fmt ./...

build:
	docker compose build

loadtest: ## restart the API without rate limits, then benchmark redirects
	RATE_CREATE_PER_MIN=0 RATE_REDIRECT_RPS=0 docker compose up -d api
	@sleep 2
	docker compose run --rm --no-deps --entrypoint /usr/local/bin/loadgen api -target http://api:8080 -c 64 -d 20s
	docker compose up -d api

clean:
	docker compose down -v
