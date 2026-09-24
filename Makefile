.PHONY: verify fmt fmt-check mod-verify test coverage vet build image image-controller image-node image-combined image-version-check release-workflow-test shellcheck actionlint helm-lint helm-template chart-render-diff complexity linux-mount-integration kind-e2e kind-mobility-e2e kind-upgrade-e2e v04-model v04-kubernetes-primitives v04-filesystem-primitives

CONTROLLER_VERSION_FILE ?= versions/controller
NODE_VERSION_FILE ?= versions/node
CONTROLLER_VERSION ?= $(shell cat $(CONTROLLER_VERSION_FILE))
NODE_VERSION ?= $(shell cat $(NODE_VERSION_FILE))
CONTROLLER_IMAGE ?= shiftpv-controller:$(CONTROLLER_VERSION)
NODE_IMAGE ?= shiftpv-node:$(NODE_VERSION)
IMAGE ?= shiftpv:dev
COVERAGE_MIN ?= 80
COVERAGE_PACKAGES := ./src/csi/... ./src/kubernetes/... ./src/lifecycle/... ./src/metrics/... ./src/mobility/... ./src/node/... ./src/pool/... ./src/volume/... ./src/webhook/... ./test/...
# Every shell script under build/ and test/ is linted; shellcheck picks each
# file's dialect from its own shebang.
SHELL_SCRIPTS := $(shell find build test -type f -name '*.sh' | LC_ALL=C sort)

# helm-template is not listed here: COVERAGE_PACKAGES already contains ./test/...,
# so coverage runs ./test/helm once with -race. The target stays for running the
# chart render contracts on their own.
verify: fmt-check mod-verify coverage vet build image-version-check release-workflow-test shellcheck actionlint helm-lint complexity v04-model

fmt:
	gofmt -w $$(find src test -name '*.go' -type f)

fmt-check:
	@test -z "$$(gofmt -l src test)" || { gofmt -l src test; exit 1; }

mod-verify:
	go mod verify

test: coverage

v04-model:
	go test -race -count=1 -v ./test/model

v04-kubernetes-primitives:
	./test/model/kubernetes-primitives.sh

v04-filesystem-primitives:
	./test/model/filesystem-primitives.sh

coverage:
	mkdir -p .tmp
	go test -race -covermode=atomic -coverprofile=.tmp/coverage.out $(COVERAGE_PACKAGES)
	go tool cover -func=.tmp/coverage.out | tee .tmp/coverage.txt
	@total=$$(awk '/^total:/ {gsub(/%/, "", $$3); print $$3}' .tmp/coverage.txt); \
		awk -v total="$${total}" -v minimum="$(COVERAGE_MIN)" 'BEGIN { \
			if (total + 0 < minimum + 0) { \
				printf "coverage %.1f%% is below %.1f%%\n", total, minimum; \
				exit 1; \
			} \
			printf "coverage %.1f%% meets %.1f%% minimum\n", total, minimum; \
		}'

vet:
	go vet ./...

build:
	go build ./src/cmd/controller ./src/cmd/node ./src/cmd/uninstall-guard ./src/cmd/volume-helper

image: image-controller image-node

image-controller: image-version-check
	docker build --target controller --build-arg CONTROLLER_VERSION=$(CONTROLLER_VERSION) -f build/package/Dockerfile -t $(CONTROLLER_IMAGE) .

image-node: image-version-check
	docker build --target node --build-arg NODE_VERSION=$(NODE_VERSION) -f build/package/Dockerfile -t $(NODE_IMAGE) .

image-combined: image-version-check
	docker build --target combined --build-arg CONTROLLER_VERSION=$(CONTROLLER_VERSION) --build-arg NODE_VERSION=$(NODE_VERSION) -f build/package/Dockerfile -t $(IMAGE) .

image-version-check:
	@for file in $(CONTROLLER_VERSION_FILE) $(NODE_VERSION_FILE); do \
		test -f "$$file" || { echo "image version file not found: $$file" >&2; exit 1; }; \
		grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$$' "$$file" || { echo "image version must use numeric major.minor.patch format: $$file" >&2; exit 1; }; \
	done

release-workflow-test:
	./test/release/version-increase.sh
	./test/release/wait-for-chart-images.sh
	./test/release/validate-artifact-lock.sh
	./test/release/resolve-latest-artifacts.sh

shellcheck:
	@test -n "$(SHELL_SCRIPTS)" || { echo 'no shell scripts were discovered under build/ or test/'; exit 1; }
	shellcheck $(SHELL_SCRIPTS)

actionlint:
	@if command -v actionlint >/dev/null 2>&1; then \
		actionlint; \
	else \
		echo "actionlint not installed; skipping local workflow lint"; \
	fi

linux-mount-integration:
	./test/integration/linux-mount/run.sh

# Runs every kind E2E scenario on one cluster. CI shards the same script with
# KIND_E2E_GROUP=g1|g2|g3, which this target passes through when it is set.
kind-e2e:
	./test/e2e/kind/run.sh

# Runs every closed-loop mobility scenario on one cluster. CI shards the same
# script with KIND_MOBILITY_GROUP=g1|g2|g3, passed through when it is set.
kind-mobility-e2e:
	./test/e2e/kind/mobility/run.sh

# Installs the artifact-lock release, upgrades in place to this checkout, and
# checks the documented StorageClass replacement. Needs Docker and kind.
kind-upgrade-e2e:
	./test/e2e/kind/upgrade/run.sh

helm-lint:
	helm lint charts/shiftpv

# Proves a chart refactor is render-neutral: templates the chart at BASE_REF and
# at this checkout for every values combination the suites use and diffs the
# bytes. Not part of verify, which has no base ref to compare against.
chart-render-diff:
	./build/ci/chart-render-diff.sh $(BASE_REF)

complexity:
	./build/ci/complexity-check.sh

# Chart render contracts live in ./test/helm as structural Go assertions, so a
# broken contract names the flag or object instead of printing "Error 1".
helm-template:
	go test -count=1 ./test/helm/...
