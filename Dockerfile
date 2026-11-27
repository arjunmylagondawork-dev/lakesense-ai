FROM python:3.11-slim

# PySpark needs a JVM
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONUNBUFFERED=1 \
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
CMD ["sh", "-c", "lakesense all && streamlit run app/streamlit_app.py --server.address=0.0.0.0 --server.port=8501"]
