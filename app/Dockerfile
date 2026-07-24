# STAGE 1: Asset Downloader (dovi_tool)
FROM debian:bullseye-slim AS builder
ARG TARGETARCH
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# dovi_tool 2.1.2 — amd64 / arm64 musl builds from quietvoid releases
RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64) DOVI_ARCH=x86_64-unknown-linux-musl ;; \
      arm64) DOVI_ARCH=aarch64-unknown-linux-musl ;; \
      *) echo "Unsupported TARGETARCH=${TARGETARCH} (need amd64 or arm64)" >&2; exit 1 ;; \
    esac; \
    curl -fL -o dovi_tool.tar.gz \
      "https://github.com/quietvoid/dovi_tool/releases/download/2.1.2/dovi_tool-2.1.2-${DOVI_ARCH}.tar.gz"; \
    tar -xzf dovi_tool.tar.gz; \
    chmod +x dovi_tool; \
    mv dovi_tool /usr/local/bin/

# STAGE 2: Runtime Application
# Build context: ./app (docker-compose: build: ./app)
FROM python:3.11-slim

# System deps (ffmpeg + mediainfo + curl for HEALTHCHECK)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    mediainfo \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Non-root HOME so gunicorn can write its control socket (avoids /.gunicorn EACCES)
ENV HOME=/tmp \
    PYTHONUNBUFFERED=1

COPY --from=builder /usr/local/bin/dovi_tool /usr/local/bin/dovi_tool

RUN mkdir -p static

# Pin Chart.js so builds are reproducible (was unpinned CDN latest).
ARG CHART_JS_VERSION=4.4.8
RUN python -c "import urllib.request; urllib.request.urlretrieve('https://cdn.jsdelivr.net/npm/chart.js@${CHART_JS_VERSION}/dist/chart.umd.min.js', 'static/chart.js')"

# Python deps from lockfile-style pin list
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App assets + source (includes CHANGELOG.md for app_version())
COPY static/*.png /app/static/
COPY static/favicon.ico /app/static/
COPY . .

RUN mkdir -p /output /tmp && chown -R 3000:1000 /app /output /tmp

USER 3000:1000

EXPOSE 6002

HEALTHCHECK --interval=30s --timeout=5s --start-period=25s --retries=3 \
  CMD curl -fsS http://127.0.0.1:6002/api/health || exit 1

CMD ["python", "-m", "gunicorn", "--bind", "0.0.0.0:6002", "--workers", "1", "--threads", "4", "--timeout", "120", "analyzer:app"]
