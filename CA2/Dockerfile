FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    tar \
    ca-certificates \
    openjdk-17-jdk-headless \
    procps \
    libsnappy1v5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

ARG TARGETARCH
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-${TARGETARCH}
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app
COPY pyproject.toml .
RUN mkdir -p darooghe
RUN pip install --upgrade pip && \
    pip install .

COPY . .

RUN find scripts/ -type f -name "*.sh" -exec chmod +x {} \;

CMD ["python", "main.py"]
