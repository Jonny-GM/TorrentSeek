VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X main.version=$(VERSION)

.PHONY: build dist test vet fmt live clean

build:
	go build -ldflags "$(LDFLAGS)" -o bin/torrentseek ./cmd/torrentseek

# Cross-compile release archives for all platforms into dist/.
dist:
	VERSION=$(VERSION) ./scripts/release-build.sh

test:
	go vet ./...
	go test -race ./...

fmt:
	gofmt -w .

# Live validation against a real deluged process (no containers).
# SWARM=1 adds the tracker+seeder stage. See test/live/run.sh.
live:
	./test/live/run.sh

clean:
	rm -rf bin dist
