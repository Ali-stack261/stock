# Stage 1: build dependencies and copy source
FROM python:3.12-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && apt-get purge -y --auto-remove perl perl-modules-* libperl5.* 2>/dev/null || true \
    && rm -rf /var/lib/apt/lists/*

RUN curl -sL -o /tmp/jdk17.tar.gz \
    "https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.20%2B8/OpenJDK17U-jdk_x64_linux_hotspot_17.0.20_8.tar.gz" \
    && mkdir -p /opt/jdk17 \
    && tar -xzf /tmp/jdk17.tar.gz -C /opt/jdk17 --strip-components=1 \
    && rm /tmp/jdk17.tar.gz

ENV JAVA_HOME=/opt/jdk17
ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY serving/ ./serving/
COPY training/ ./training/
COPY monitoring/ ./monitoring/
COPY streaming/ ./streaming/

# Stage 2: runtime image — copy only the venv and source
FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && apt-get purge -y --auto-remove perl perl-modules-* libperl5.* 2>/dev/null || true \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/jdk17 /opt/jdk17
ENV JAVA_HOME=/opt/jdk17
ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /app
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /build/ ./

EXPOSE 8000
CMD ["uvicorn", "serving.app:app", "--host", "0.0.0.0", "--port", "8000"]
