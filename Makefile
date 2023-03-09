VERSION             := $(shell cat VERSION)
BIN_DIR             := bin
REGISTRY            = docker.io/miaojun/etcd-ops
IMAGE_TAG           := $(VERSION)

IMG := ${REGISTRY}:${IMAGE_TAG}

.PHONY: build
build:
	@env GO111MODULE=on go mod tidy -v
	@./build.sh

.PHONY: image-build
image-build:
	@docker build -t ${IMG} .

.PHONY: image-push
image-push:
	@if ! docker images $(REGISTRY) | awk '{ print $$2 }' | grep -q -F $(IMAGE_TAG); then echo "$(REGISTRY) version $(IMAGE_TAG) is not yet built. Please run 'make docker-image'"; false; fi
	@docker push ${IMG}

.PHONY: clean
clean:
	@rm -rf $(BIN_DIR)/
