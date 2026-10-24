# =============================================================================
#  ZenInbox — Multi-Stage Dockerfile (FastAPI + Generative UI)
#  Base: python:3.10-slim  ·  Exposed port: 8000
# =============================================================================

# ---------------------------------------------------------------------------
# Stage 1 — Builder: install Python dependencies into a virtual-env
# ---------------------------------------------------------------------------
FROM python:3.10-slim AS builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential && \
    rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# ---------------------------------------------------------------------------
# Stage 2 — Runtime: lean image with only what we need
# ---------------------------------------------------------------------------
FROM python:3.10-slim AS runtime

LABEL maintainer="ZenInbox <zeninbox@local>"
LABEL description="Local privacy-first email agent — FastAPI + Ollama (Gemma 2) + Generative UI"

# Copy the pre-built virtual-env from the builder stage
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# App directory
WORKDIR /app
COPY app.py .
COPY ui.html .

# Persistent volume for the SQLite database
VOLUME ["/app/data"]
ENV ZENINBOX_DB="/app/data/zen_inbox.db"
ENV PORT="8000"

# Expose the application port
EXPOSE 8000

# Health check: verify FastAPI health endpoint is responding
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

ENTRYPOINT ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
