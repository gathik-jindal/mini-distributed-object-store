# Mini Distributed Object Store -- common tasks.  `make help` lists them.
GO      ?= go
BIN     := bin
COMPOSE := docker compose

.PHONY: help build test race vet fmt up down reset logs ps samples demo integration integration-slow clean

help: ## show this help
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

build: ## build controller, worker and dosctl into ./bin
	CGO_ENABLED=0 $(GO) build -o $(BIN)/ ./cmd/...

test: ## unit + in-process cluster tests
	$(GO) test ./...

race: ## tests with the race detector
	$(GO) test -race ./...

vet: ## go vet + gofmt check
	$(GO) vet ./...
	@test -z "$$(gofmt -l .)" || (gofmt -l . && false)

fmt: ## gofmt everything
	gofmt -w .

up: ## build images and start controller + 4 workers
	$(COMPOSE) up -d --build

down: ## stop the stack (keeps data)
	$(COMPOSE) down

reset: ## stop the stack and wipe all volumes
	$(COMPOSE) down -v

logs: ## follow controller events
	$(COMPOSE) logs -f controller

ps: ## container status
	$(COMPOSE) ps

samples: ## generate demo files into ./samples
	scripts/gen_samples.sh

demo: build ## guided walkthrough of the six required demos
	PATH="$(CURDIR)/$(BIN):$$PATH" scripts/demo.sh

integration: ## integration tests against the running stack
	$(GO) test -tags integration -count=1 ./tests/integration/...

integration-slow: ## + real docker stop/start of worker2 (~1 min)
	DOS_SLOW=1 $(GO) test -tags integration -count=1 -v ./tests/integration/...

clean: ## remove build output
	rm -rf ./$(BIN)
