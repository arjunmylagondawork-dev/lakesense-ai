FROM python:3.11-slim

# PySpark needs a JVM. default-jre-headless works on any Debian release and on
# both amd64 and arm64 (Apple Silicon); Spark finds `java` on the PATH.
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*
ENV PYTHONUNBUFFERED=1 \
    LAKESENSE_HOME=/app

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir ".[app,openai,anthropic,bedrock]"
COPY config ./config
COPY app ./app
COPY dags ./dags

EXPOSE 8501
# Build the lakehouse once, then serve the UI
# Delta jars are fetched from Maven on first start and cached in the ivy volume
CMD ["sh", "-c", "lakesense all && streamlit run app/streamlit_app.py --server.address=0.0.0.0 --server.port=8501"]
