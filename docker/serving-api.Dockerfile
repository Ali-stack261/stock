# Stage 1: build dependencies and copy source
FROM eclipse-temurin:17-jdk-jammy AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.12 python3.12-distutils python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY requirements.txt .
RUN python3.12 -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

COPY serving/ ./serving/
COPY training/ ./training/
COPY monitoring/ ./monitoring/
COPY streaming/ ./streaming/

# Stage 2: runtime image — copy only the venv and source
FROM eclipse-temurin:17-jdk-jammy

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.12 python3.12-distutils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /build/serving/ ./serving/
COPY --from=builder /build/training/ ./training/
COPY --from=builder /build/monitoring/ ./monitoring/
COPY --from=builder /build/streaming/ ./streaming/

ENV PATH="/opt/venv/bin:$PATH"

EXPOSE 8000
CMD ["uvicorn", "serving.app:app", "--host", "0.0.0.0", "--port", "8000"]
