# Image URL to use all building/pushing image targets
VER ?= 1.0.0
IMG ?= registry.cn-beijing.aliyuncs.com/data-loom/data-loom:${VER}

# application name
APP ?= data-loom

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

export IMG
export APP

# Setting SHELL to bash allows bash commands to be executed by recipes.
# This is a requirement for 'setup-envtest.sh' in the test target.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: help

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk commands is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

##@ Build

.PHONY: build
build: fmt ## Build binary.
	go build -ldflags "-X 'github.com/sunkaimr/data-loom/cmd.version=${VER}' \
                       -X 'github.com/sunkaimr/data-loom/cmd.goVersion=$$(go version)' \
                       -X 'github.com/sunkaimr/data-loom/cmd.gitCommit=$$(git show -s --format=%H)' \
                       -X 'github.com/sunkaimr/data-loom/cmd.buildTime=$$(date +'%Y-%m-%d %H:%M:%S')'" \
                       -o ${APP} main.go

.PHONY: run
run: fmt vet ## Run the application on your host.
	go run ./main.go server

.PHONY: docker-build-test
docker-build-test: build   ## Build docker image by binary and Dockerfile1
	docker build -t ${IMG} . -f Dockerfile1 --network=host

.PHONY: docker-build
docker-build:   ## Build docker image by Dockerfile
	docker build -t ${IMG} . -f Dockerfile

.PHONY: docker-push
docker-push:   ## Push docker image to registry
	docker push ${IMG}

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

#.PHONY: deploy
#deploy:  ## Deploy the application as a container to the k8s cluster
#	kubectl apply -f deploy/logrotate.yaml
#
#.PHONY: undeploy
#undeploy:  ## Delete the application from the k8s cluster
#	kubectl delete -f deploy/logrotate.yaml

##@ Clean

.PHONY: clean
clean:  ## Remove Build binary and images
	rm -rf ${APP}
	docker rmi ${IMG}
