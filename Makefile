SHELL=/bin/bash
short_ver = 2.1.3
long_ver = $(shell git describe --long 2>/dev/null || echo $(short_ver)-0-unknown-g`git describe --always`)

USE_RE2=${USE_RE2}

all: py-egg

# --- Go (Python-to-Go migration, docs/plan-python-to-go-migration.md) ---
GO ?= go
# Mirrors the Python Makefile's short_ver/long_ver logic: `git describe
# --tags` with a VERSION env override.
# Note: the fallback $(short_ver)-unknown diverges from Python's long_ver
# fallback $(short_ver)-0-unknown-g<hash> (Python runs a second `git
# describe --always` when the first fails); here there is simply no
# describe output outside a git checkout.
VERSION ?= $(shell git describe --tags 2>/dev/null || echo $(short_ver)-unknown)
GO_LDFLAGS = -X github.com/Aiven-Open/journalpump/internal/version.Version=$(VERSION)

PYTHON ?= python3
PYLINT_DIRS = journalpump/ test/ systest/
MYPY_TARGETS = journalpump/ test/ systest/ scripts/ conftest.py setup.py

.PHONY: unittest
unittest:
	$(PYTHON) -m pytest -vv test/

.PHONY: pylint
pylint:
	$(PYTHON) -m pylint --rcfile .pylintrc $(PYLINT_DIRS) || exit $$(($$?&2))  # only fail on errors

.PHONY: mypy
mypy:
	$(PYTHON) -m mypy $(MYPY_TARGETS)

.PHONY: systest
systest:
	$(PYTHON) -m pytest -vv systest/

.PHONY: systest-on-podman
systest-on-podman:
	./scripts/systest-on-podman

py-egg:
	VERSION=$(shell git describe --tags) $(PYTHON) setup.py bdist_egg

.PHONY: coverage
coverage:
	$(PYTHON) -m pytest $(PYTEST_ARG) --cov-report term-missing --cov-report xml:coverage.xml \
		--cov journalpump test/

.PHONY: build-go
build-go:
	$(GO) build -ldflags "$(GO_LDFLAGS)" -o build/journalpump ./cmd/journalpump

# Unit tests live next to the code (*_test.go); systest files carry the
# `systest` build tag and are excluded here.
.PHONY: unittest-go
unittest-go:
	$(GO) test -race ./...

.PHONY: systest-go
systest-go:
	$(GO) test -race -tags systest ./systest-go/...

# Provisions a throwaway Kafka 4.0 KRaft broker on podman, points
# JOURNALPUMP_KAFKA_BROKER at it, and runs the systest suite UNCACHED so the
# opt-in kafka e2e/interop tests execute against a real broker instead of
# skipping. See scripts/kafka-e2e-go for the overrides.
.PHONY: systest-go-kafka
systest-go-kafka:
	./scripts/kafka-e2e-go

# Lint per decision D10 (golangci-lint incl. gocyclo, ineffassign, revive,
# staticcheck, errcheck, govet, gocritic); the golangci config is checked
# in with the CI workflow (plan Task 2).
.PHONY: lint-go
lint-go:
	$(GO) vet ./...
	golangci-lint run ./...

clean:
	$(RM) -r *.egg-info/ build/ dist/
	$(RM) ../journalpump_* test-*.xml coverage.xml

# The golang marker in the version makes the Go build distinguishable from
# the Python package built from the main branch (the rpm release ID gains
# the equivalent marker in journalpump.spec).  The `~` sorts the golang
# build below the pristine upstream version, the Debian convention for a
# non-upstream build variant.
# -B builds the architecture-dependent binary only: the Go package is
# Architecture: any (the Python package was Architecture: all, hence its
# historical -A).
deb:
	cp debian/changelog.in debian/changelog
	dch -v $(long_ver)~golang "Automatically built package (golang build)"
	dpkg-buildpackage -B -uc -us

rpm:
	git archive --output=journalpump-rpm-src.tar.gz --prefix=journalpump/ HEAD
	rpmbuild -bb journalpump.spec \
		--define '_sourcedir $(shell pwd)' \
                --define '_topdir $(shell pwd)/rpm' \
		--define 'major_version $(shell git describe --tags --abbrev=0 | cut -f1-)' \
		--define 'minor_version $(subst -,.,$(shell git describe --tags --long | cut -f2- -d-))'
	$(RM) journalpump-rpm-src.tar.gz

# Package build smoke test (plan Task 27): build the rpm and deb and assert
# the Go binary and the unchanged systemd unit are packaged under the golang
# release marker.  See scripts/check-packaging.
.PHONY: check-packaging
check-packaging:
	./scripts/check-packaging

# Install-and-start check in the podman systest environment: install the
# built rpm in a privileged Fedora container with systemd and assert
# journalpump.service reaches active (Type=notify READY=1).  See
# scripts/systest-package-on-podman.
.PHONY: systest-package-on-podman
systest-package-on-podman:
	./scripts/systest-package-on-podman

build-dep-fed:
	sudo dnf -y --best --allowerasing install \
		python3-flake8 python3-kafka python3-pytest python3-pylint \
		python3-requests python3-responses systemd-python3 python3-botocore \
		python3-websockets python3-aiohttp-socks python3-snappy \
		python3-google-api-client python3-google-auth

build-dep-deb:
	sudo apt-get install \
		build-essential devscripts dh-systemd \
		python-all python-setuptools python3-systemd python3-kafka \
		python3-websockets python3-aiohttp-socks python3-snappy \
		python3-botocore python3-googleapi python3-google-auth


.PHONY: rpm
