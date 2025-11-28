# syntax=docker/dockerfile:1
# Multi-stage Dockerfile for Salesforce SERP enrichment

ARG PYTHON_VERSION=3.11
FROM python:${PYTHON_VERSION}-slim AS base

# Avoid interactive prompts & set environment
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_ROOT_USER_ACTION=ignore

# System deps (add curl for health/debug; build deps for pandas if needed)
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies separately for better layer caching
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY account_fields.py main.py tools/sf_cleaner.py ./
COPY fetcher ./fetcher

# Non-root user for security
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

# Default env variable placeholders (override at runtime)
ENV SF_DOMAIN=login \
    LOG_LEVEL=INFO \
    SERP_WORKERS=5 \
    SF_WORKERS=6

# Entrypoint: run the sf_cleaner CLI by default in dry-run mode
# Override with: docker run image python main.py enrich ...
ENTRYPOINT ["python", "tools/sf_cleaner.py"]
CMD ["--log-level", "INFO"]

# Example run (dry-run enrichment):
# docker build -t serp-enrich .
# docker run --rm -e SERPAPI_API_KEY=XXXX -e SF_USERNAME=... -e SF_PASSWORD=... -e SF_SECURITY_TOKEN=... serp-enrich --limit 100
