FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    tar \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_URL=https://github.com/adoptium/temurin23-binaries/releases/download/jdk-23.0.2%2B7/OpenJDK23U-debugimage_aarch64_alpine-linux_hotspot_23.0.2_7.tar.gz

RUN mkdir -p /opt/java && \
    wget -q ${JAVA_URL} -O /tmp/java.tar.gz && \
    tar -xzf /tmp/java.tar.gz -C /opt/java && \
    rm /tmp/java.tar.gz

ENV JAVA_HOME=/opt/java/jdk-23.0.2+7
ENV PATH="${JAVA_HOME}/bin:$PATH"

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN pip install --no-cache-dir -e .

RUN chmod +x scripts/*.py

EXPOSE 4040

ENV PYTHONPATH=/app

CMD []
