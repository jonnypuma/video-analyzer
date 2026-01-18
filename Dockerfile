# STAGE 1: Asset Downloader
FROM debian:bullseye-slim as builder
RUN apt-get update && apt-get install -y curl tar ca-certificates

# Robustly download dovi_tool (v2.1.2) - Change URL if architecture differs (e.g. arm64 for Mac M1/Pi)
# Using -f (fail), -L (follow redirects), -S (show errors)
RUN curl -fL -o dovi_tool.tar.gz https://github.com/quietvoid/dovi_tool/releases/download/2.1.2/dovi_tool-2.1.2-x86_64-unknown-linux-musl.tar.gz \
    && tar -xzvf dovi_tool.tar.gz \
    && chmod +x dovi_tool \
    && mv dovi_tool /usr/local/bin/

# STAGE 2: Runtime Application
FROM python:3.11-slim

# 1. Install System Dependencies (ffmpeg + mediainfo + curl)
RUN apt-get update && apt-get install -y \
    ffmpeg \
    mediainfo \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 2. Set working directory
WORKDIR /app

# 3. Copy dovi_tool from builder stage
COPY --from=builder /usr/local/bin/dovi_tool /usr/local/bin/dovi_tool

# 4. Create static directory
RUN mkdir -p static

# 5. Download Chart.js
RUN python -c "import urllib.request; urllib.request.urlretrieve('https://cdn.jsdelivr.net/npm/chart.js', 'static/chart.js')"

# 6. Install Python requirements (Added pymediainfo)
RUN pip install --no-cache-dir flask gunicorn apscheduler pymediainfo

# 7. COPY ASSETS
COPY static/*.png /app/static/
COPY static/favicon.ico /app/static/

# 8. COPY APPLICATION
COPY . .

# 9. PERMISSIONS
RUN mkdir -p /output && chown -R 3000:1000 /app /output

# 10. User
USER 3000:1000

# 11. Expose
EXPOSE 6002

# 12. Start
CMD ["python", "-m", "gunicorn", "--bind", "0.0.0.0:6002", "--workers", "1", "--threads", "4", "--timeout", "120", "analyzer:app"]