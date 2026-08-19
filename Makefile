MODULES := proto services/api services/worker tools/pdfgen

.PHONY: help up down build test vet proto pdfgen clean

help: ## list targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

up: ## build and start the whole system (docker compose)
	docker compose up --build -d --wait

down: ## stop the system, keep data volumes
	docker compose down

build: ## go build all modules
	@for m in $(MODULES); do (cd ./$$m && go build ./...) || exit 1; done

test: ## go test all modules
	@for m in $(MODULES); do (cd ./$$m && go test ./...) || exit 1; done

vet: ## go vet all modules
	@for m in $(MODULES); do (cd ./$$m && go vet ./...) || exit 1; done

proto: ## regenerate gRPC code from proto/jobs.proto (needs protoc + go plugins)
	protoc --proto_path=proto \
		--go_out=proto/gen --go_opt=module=github.com/anatolyt/interview-mls/proto/gen \
		--go-grpc_out=proto/gen --go-grpc_opt=module=github.com/anatolyt/interview-mls/proto/gen \
		proto/jobs.proto

COUNT ?= 3
PAGES ?= 2
ROWS  ?= 25
SEED  ?= 42

pdfgen: ## generate sample pdfs, e.g. make pdfgen COUNT=10 SEED=7
	go run ./tools/pdfgen/cmd/pdfgen --out ./samples --count $(COUNT) --pages $(PAGES) --rows $(ROWS) --seed $(SEED)

clean: ## stop the system and delete data volumes
	docker compose down -v
