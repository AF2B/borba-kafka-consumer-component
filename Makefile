# The Borba repository standard: one command for everything a change must pass.
# The pipelines run these same targets, so a red build reproduces on a laptop.
# This file is stamped from borba-tooling; change the standard there.

SHELL := /bin/bash
.DEFAULT_GOAL := help

CLOJURE           ?= clojure
BB                ?= bb
DOCKER            ?= docker
GITLEAKS_IMAGE    ?= ghcr.io/gitleaks/gitleaks:v8.30.1
OSV_SCANNER_IMAGE ?= ghcr.io/google/osv-scanner:v2.6.0
TAG               ?= $(shell git describe --tags --abbrev=0 2>/dev/null)

.PHONY: help
help: ## List the targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: deps
deps: ## Resolve and download every dependency, of every alias and of the tooling
	for alias in test coverage lint fmt build outdated; do $(CLOJURE) -P -M:$$alias || exit 1; done
	$(BB) -e nil

.PHONY: lint
lint: ## Lint with clj-kondo, failing on any warning
	$(CLOJURE) -M:lint

.PHONY: fmt-check
fmt-check: ## Check the formatting with cljfmt
	$(CLOJURE) -M:fmt check

.PHONY: fmt
fmt: ## Format the sources with cljfmt
	$(CLOJURE) -M:fmt fix

.PHONY: conventions
conventions: ## Check docstrings, arglist layout and line length
	$(BB) -x borba.tooling.conventions/check

.PHONY: reflection
reflection: ## Fail on reflection warnings in the project's own namespaces
	$(BB) -x borba.tooling.reflection/check

.PHONY: test
test: ## Run the unit tests
	$(CLOJURE) -M:test --focus :unit

.PHONY: test-integration
test-integration: ## Run the integration tests, which need the services they name
	$(CLOJURE) -M:test --focus :integration

.PHONY: coverage
coverage: ## Run the unit tests with coverage and enforce the floor
	$(CLOJURE) -M:coverage

.PHONY: sbom
sbom: ## Write the CycloneDX bill of materials to target/sbom.cdx.json
	$(BB) -x borba.tooling.sbom/generate

.PHONY: audit
audit: sbom ## Scan the bill of materials for known vulnerabilities
	$(DOCKER) run --rm --volume "$(CURDIR)/target:/sbom:ro" $(OSV_SCANNER_IMAGE) \
		scan source -L /sbom/sbom.cdx.json

.PHONY: secret-scan
secret-scan: ## Scan the whole git history for committed secrets
	$(DOCKER) run --rm --volume "$(CURDIR):/repo:ro" $(GITLEAKS_IMAGE) \
		git /repo --redact --no-banner

.PHONY: build
build: ## Build the library jar and its pom under target/
	$(CLOJURE) -T:build jar

.PHONY: outdated
outdated: ## Report dependencies that have a newer version
	$(CLOJURE) -M:outdated

.PHONY: release-check
release-check: ## Check that the tag in TAG can be released
	$(BB) -x borba.tooling.release/check --tag $(TAG)

.PHONY: check
check: lint fmt-check conventions reflection test coverage ## What a change must pass before a push

.PHONY: ci
ci: check audit secret-scan build ## Everything the pipelines enforce

.PHONY: clean
clean: ## Remove build output and caches
	rm -rf target .cpcache
