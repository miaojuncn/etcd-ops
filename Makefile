VERSION             := v1.0
BIN_DIR             := bin
REGISTRY            = miaojun/etcd-ops
VCS 				= github.com/miaojuncn/etcd-ops
IMG 				:= $(REGISTRY):$(VERSION)
GIT_SHA				= $(shell git rev-parse --short HEAD || echo "GitNotFound")

##@ General

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

##@ Build

.PHONY: build
build: fmt vet ## Build binary.
	env CGO_ENABLED=0 go build \
      -a -o $(BIN_DIR)/etcd-ops \
      -ldflags "-X $(VCS)/pkg/version.Version=$(VERSION) -X $(VCS)/pkg/version.GitSHA=$(GIT_SHA)" \
      main.go

.PHONY: image-build
image-build: ## Build docker image.
	@docker build -t $(IMG) .

.PHONY: image-push
image-push: ## Push docker image.
	@if ! docker images $(REGISTRY) | awk '{ print $$2 }' | grep -q -F $(IMAGE_TAG); then echo "$(REGISTRY) version $(VERSION) is not yet built. Please run 'make image-build'"; false; fi
	@docker push $(IMG)

##@ Deployment

.PHONY: deploy
deploy: ## Deploy etcd-ops.
	cd manifests && kubectl apply -f .
