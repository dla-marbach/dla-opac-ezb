# Base Image mit festem Versionstag, Updates über Dependabot.
FROM mcr.microsoft.com/devcontainers/python:3.3.2-3.13-trixie

# go-task mit fester Version, Prüfsumme wird geprüft. Dependabot erfasst die
# Version nicht, Updates von Hand: https://github.com/go-task/task/releases
ARG TASK_VERSION=3.54.0
WORKDIR /tmp/go-task
RUN set -eu; \
    arch="$(dpkg --print-architecture)"; \
    base="https://github.com/go-task/task/releases/download/v${TASK_VERSION}"; \
    curl -fsSLO "${base}/task_linux_${arch}.tar.gz"; \
    curl -fsSLO "${base}/task_checksums.txt"; \
    sha256sum --check --ignore-missing task_checksums.txt; \
    tar -xzf "task_linux_${arch}.tar.gz" -C /usr/local/bin task; \
    rm -rf /tmp/go-task
WORKDIR /

USER vscode
