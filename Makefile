.PHONY: test coverage fmt-check fmt-fix lint build clean deps help

SERVICE_NAME = borba-kafka-consumer-component
VERSION      = $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")

test:
	@echo "▶ Running tests..."
	@clojure -M:test -m kaocha.runner
	@echo "✅ Tests passed"

coverage:
	@echo "▶ Running tests with coverage..."
	@clojure -M:test -m kaocha.runner --plugin kaocha.plugin/cloverage --cloverage-output target/coverage
	@echo "✅ Coverage: target/coverage/index.html"

fmt-check:
	@echo "▶ Checking formatting (src/ test/)..."
	@clojure -M:fmt-check
	@echo "✅ Formatting OK"

fmt-fix:
	@echo "▶ Fixing formatting (src/ test/)..."
	@clojure -M:fmt-fix
	@echo "✅ Formatting fixed"

lint:
	@echo "▶ Linting (src/)..."
	@clojure -M:lint
	@echo "✅ Lint OK"

build:
	@echo "▶ Building library JAR..."
	@APP_VERSION=$(VERSION) clojure -T:build jar

clean:
	@echo "▶ Cleaning..."
	@rm -rf .cpcache target
	@echo "✅ Clean"

deps:
	@echo "▶ Resolving dependencies..."
	@clojure -P
	@clojure -P -M:test
	@echo "✅ Dependencies resolved"

help:
	@echo ""
	@echo "  $(SERVICE_NAME)"
	@echo ""
	@echo "  test        Run tests"
	@echo "  coverage    Tests + coverage report (target/coverage/)"
	@echo "  fmt-check   Check formatting"
	@echo "  fmt-fix     Fix formatting in src/ and test/"
	@echo "  lint        Lint src/"
	@echo "  build       Build library JAR → target/"
	@echo "  clean       Remove .cpcache and target/"
	@echo "  deps        Resolve and cache all dependencies"
	@echo ""
